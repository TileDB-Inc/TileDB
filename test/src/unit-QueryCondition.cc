/**
 * @file unit-QueryCondition.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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

#include "test/src/helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/query_condition_combination_op.h"
#include "tiledb/sm/enums/query_condition_op.h"
#include "tiledb/sm/query/query_condition.h"

#include <catch.hpp>
#include <iostream>

using namespace tiledb::sm;

TEST_CASE(
    "QueryCondition: Test default constructor",
    "[QueryCondition][default_constructor]") {
  QueryCondition query_condition;
  REQUIRE(query_condition.empty());
  REQUIRE(query_condition.field_names().empty());

  ArraySchema array_schema;
  std::vector<ResultCellSlab> result_cell_slabs;
  REQUIRE(query_condition.apply(array_schema, result_cell_slabs, 1).ok());
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

TEST_CASE("QueryCondition: Test char", "[QueryCondition][char_value]") {
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
}

TEST_CASE(
    "QueryCondition: Test AST construction, basic",
    "[QueryCondition][ast][api]") {
  std::string field_name = "x";
  int val = 5;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition.ast()) ==
      "x LT 05 00 00 00");
}

TEST_CASE(
    "Query Condition: Test AST construction, basic AND combine",
    "[QueryCondition][ast][api]") {
  // AND combine.
  std::string field_name = "x";
  int val = 5;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition.ast()) ==
      "x LT 05 00 00 00");

  std::string field_name1 = "y";
  int val1 = 3;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::GT)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition1.ast()) ==
      "y GT 03 00 00 00");

  QueryCondition combined_and;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::AND, &combined_and)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_and.ast()) ==
      "(x LT 05 00 00 00 AND y GT 03 00 00 00)");
}

TEST_CASE(
    "Query Condition: Test AST construction, basic OR combine",
    "[QueryCondition][ast][api]") {
  // OR combine
  std::string field_name = "x";
  int val = 5;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition.ast()) ==
      "x LT 05 00 00 00");

  std::string field_name1 = "y";
  int val1 = 3;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::GT)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition1.ast()) ==
      "y GT 03 00 00 00");

  QueryCondition combined_or;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_or.ast()) ==
      "(x LT 05 00 00 00 OR y GT 03 00 00 00)");
}

TEST_CASE(
    "Query Condition: Test AST construction, tree structure, AND of 2 OR ASTs",
    "[QueryCondition][ast][api]") {
  // First OR compound AST.
  std::string field_name = "x";
  int val = 5;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition.ast()) ==
      "x LT 05 00 00 00");

  std::string field_name1 = "y";
  int val1 = 3;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::GT)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition1.ast()) ==
      "y GT 03 00 00 00");

  QueryCondition combined_or;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_or.ast()) ==
      "(x LT 05 00 00 00 OR y GT 03 00 00 00)");

  // Second OR compound AST.
  std::string field_name2 = "a";
  int val2 = 9;
  QueryCondition query_condition2;
  REQUIRE(query_condition2
              .init(
                  std::string(field_name2),
                  &val2,
                  sizeof(int),
                  QueryConditionOp::EQ)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition2.ast()) ==
      "a EQ 09 00 00 00");

  std::string field_name3 = "b";
  int val3 = 1;
  QueryCondition query_condition3;
  REQUIRE(query_condition3
              .init(
                  std::string(field_name3),
                  &val3,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition3.ast()) ==
      "b NE 01 00 00 00");

  QueryCondition combined_or1;
  REQUIRE(
      query_condition2
          .combine(
              query_condition3, QueryConditionCombinationOp::OR, &combined_or1)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_or1.ast()) ==
      "(a EQ 09 00 00 00 OR b NE 01 00 00 00)");

  QueryCondition combined_and;
  REQUIRE(combined_or
              .combine(
                  combined_or1, QueryConditionCombinationOp::AND, &combined_and)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_and.ast()) ==
      "((x LT 05 00 00 00 OR y GT 03 00 00 00) AND (a EQ 09 00 00 00 OR b NE "
      "01 00 00 00))");
}

TEST_CASE(
    "Query Condition: Test AST construction, tree structure, OR of 2 AND ASTs",
    "[QueryCondition][ast][api]") {
  // First AND compound AST.
  std::string field_name = "x";
  int val = 5;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition.ast()) ==
      "x LT 05 00 00 00");

  std::string field_name1 = "y";
  int val1 = 3;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::GT)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition1.ast()) ==
      "y GT 03 00 00 00");

  QueryCondition combined_and;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::AND, &combined_and)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_and.ast()) ==
      "(x LT 05 00 00 00 AND y GT 03 00 00 00)");

  // Second AND compound AST.
  std::string field_name2 = "a";
  int val2 = 9;
  QueryCondition query_condition2;
  REQUIRE(query_condition2
              .init(
                  std::string(field_name2),
                  &val2,
                  sizeof(int),
                  QueryConditionOp::EQ)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition2.ast()) ==
      "a EQ 09 00 00 00");

  std::string field_name3 = "b";
  int val3 = 1;
  QueryCondition query_condition3;
  REQUIRE(query_condition3
              .init(
                  std::string(field_name3),
                  &val3,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition3.ast()) ==
      "b NE 01 00 00 00");

  QueryCondition combined_and1;
  REQUIRE(query_condition2
              .combine(
                  query_condition3,
                  QueryConditionCombinationOp::AND,
                  &combined_and1)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_and1.ast()) ==
      "(a EQ 09 00 00 00 AND b NE 01 00 00 00)");

  QueryCondition combined_or;
  REQUIRE(
      combined_and
          .combine(combined_and1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_or.ast()) ==
      "((x LT 05 00 00 00 AND y GT 03 00 00 00) OR (a EQ 09 00 00 00 AND b "
      "NE 01 00 00 00))");
}

TEST_CASE(
    "Query Condition: Test AST construction, tree structure with same "
    "combining operator, OR of 2 OR ASTs",
    "[QueryCondition][ast][api]") {
  // First OR compound AST.
  std::string field_name = "x";
  int val = 5;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition.ast()) ==
      "x LT 05 00 00 00");

  std::string field_name1 = "y";
  int val1 = 3;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::GT)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition1.ast()) ==
      "y GT 03 00 00 00");

  QueryCondition combined_or;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_or.ast()) ==
      "(x LT 05 00 00 00 OR y GT 03 00 00 00)");

  // Second OR compound AST.
  std::string field_name2 = "a";
  int val2 = 9;
  QueryCondition query_condition2;
  REQUIRE(query_condition2
              .init(
                  std::string(field_name2),
                  &val2,
                  sizeof(int),
                  QueryConditionOp::EQ)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition2.ast()) ==
      "a EQ 09 00 00 00");

  std::string field_name3 = "b";
  int val3 = 1;
  QueryCondition query_condition3;
  REQUIRE(query_condition3
              .init(
                  std::string(field_name3),
                  &val3,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition3.ast()) ==
      "b NE 01 00 00 00");

  QueryCondition combined_or1;
  REQUIRE(
      query_condition2
          .combine(
              query_condition3, QueryConditionCombinationOp::OR, &combined_or1)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_or1.ast()) ==
      "(a EQ 09 00 00 00 OR b NE 01 00 00 00)");

  QueryCondition combined_or2;
  REQUIRE(
      combined_or
          .combine(combined_or1, QueryConditionCombinationOp::OR, &combined_or2)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_or2.ast()) ==
      "(x LT 05 00 00 00 OR y GT 03 00 00 00 OR a EQ 09 00 00 00 OR b NE 01 "
      "00 00 00)");
}

TEST_CASE(
    "Query Condition: Test AST construction, tree structure with same "
    "combining operator, AND of 2 AND ASTs",
    "[QueryCondition][ast][api]") {
  // AND of 2 AND ASTs.
  // First AND compound AST.
  std::string field_name = "x";
  int val = 5;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition.ast()) ==
      "x LT 05 00 00 00");

  std::string field_name1 = "y";
  int val1 = 3;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::GT)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition1.ast()) ==
      "y GT 03 00 00 00");

  QueryCondition combined_and;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::AND, &combined_and)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_and.ast()) ==
      "(x LT 05 00 00 00 AND y GT 03 00 00 00)");

  // Second AND compound AST.
  std::string field_name2 = "a";
  int val2 = 9;
  QueryCondition query_condition2;
  REQUIRE(query_condition2
              .init(
                  std::string(field_name2),
                  &val2,
                  sizeof(int),
                  QueryConditionOp::EQ)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition2.ast()) ==
      "a EQ 09 00 00 00");

  std::string field_name3 = "b";
  int val3 = 1;
  QueryCondition query_condition3;
  REQUIRE(query_condition3
              .init(
                  std::string(field_name3),
                  &val3,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition3.ast()) ==
      "b NE 01 00 00 00");

  QueryCondition combined_and1;
  REQUIRE(query_condition2
              .combine(
                  query_condition3,
                  QueryConditionCombinationOp::AND,
                  &combined_and1)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_and1.ast()) ==
      "(a EQ 09 00 00 00 AND b NE 01 00 00 00)");

  QueryCondition combined_and2;
  REQUIRE(
      combined_and
          .combine(
              combined_and1, QueryConditionCombinationOp::AND, &combined_and2)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_and2.ast()) ==
      "(x LT 05 00 00 00 AND y GT 03 00 00 00 AND a EQ 09 00 00 00 AND b NE "
      "01 00 00 00)");
}

TEST_CASE(
    "Query Condition: Test AST construction, adding simple clauses to AND tree",
    "[QueryCondition][ast][api]") {
  // foo != 1 && foo != 3 && foo != 5 && foo != 7 && foo != 9
  std::string field_name1 = "foo";
  int val1 = 1;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition1.ast()) ==
      "foo NE 01 00 00 00");

  std::string field_name2 = "foo";
  int val2 = 3;
  QueryCondition query_condition2;
  REQUIRE(query_condition2
              .init(
                  std::string(field_name2),
                  &val2,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition2.ast()) ==
      "foo NE 03 00 00 00");

  std::string field_name3 = "foo";
  int val3 = 5;
  QueryCondition query_condition3;
  REQUIRE(query_condition3
              .init(
                  std::string(field_name3),
                  &val3,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition3.ast()) ==
      "foo NE 05 00 00 00");

  std::string field_name4 = "foo";
  int val4 = 7;
  QueryCondition query_condition4;
  REQUIRE(query_condition4
              .init(
                  std::string(field_name4),
                  &val4,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition4.ast()) ==
      "foo NE 07 00 00 00");

  std::string field_name5 = "foo";
  int val5 = 9;
  QueryCondition query_condition5;
  REQUIRE(query_condition5
              .init(
                  std::string(field_name5),
                  &val5,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition5.ast()) ==
      "foo NE 09 00 00 00");

  QueryCondition combined_and1;
  REQUIRE(query_condition1
              .combine(
                  query_condition2,
                  QueryConditionCombinationOp::AND,
                  &combined_and1)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_and1.ast()) ==
      "(foo NE 01 00 00 00 AND foo NE 03 00 00 00)");
  QueryCondition combined_and2;
  REQUIRE(combined_and1
              .combine(
                  query_condition3,
                  QueryConditionCombinationOp::AND,
                  &combined_and2)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_and2.ast()) ==
      "(foo NE 01 00 00 00 AND foo NE 03 00 00 00 AND foo NE 05 00 00 00)");
  QueryCondition combined_and3;
  REQUIRE(combined_and2
              .combine(
                  query_condition4,
                  QueryConditionCombinationOp::AND,
                  &combined_and3)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_and3.ast()) ==
      "(foo NE 01 00 00 00 AND foo NE 03 00 00 00 AND foo NE 05 00 00 00 AND "
      "foo NE 07 00 00 00)");
  QueryCondition combined_and4;
  REQUIRE(combined_and3
              .combine(
                  query_condition5,
                  QueryConditionCombinationOp::AND,
                  &combined_and4)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_and4.ast()) ==
      "(foo NE 01 00 00 00 AND foo NE 03 00 00 00 AND foo NE 05 00 00 00 AND "
      "foo NE 07 00 00 00 AND foo NE 09 00 00 00)");
}

TEST_CASE(
    "Query Condition: Test AST construction, adding simple clauses to OR tree",
    "[QueryCondition][ast][api]") {
  // foo = 0 || foo = 2 || foo = 4 || foo = 6 || foo = 8s
  std::string field_name1 = "foo";
  int val1 = 0;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::EQ)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition1.ast()) ==
      "foo EQ 00 00 00 00");

  std::string field_name2 = "foo";
  int val2 = 2;
  QueryCondition query_condition2;
  REQUIRE(query_condition2
              .init(
                  std::string(field_name2),
                  &val2,
                  sizeof(int),
                  QueryConditionOp::EQ)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition2.ast()) ==
      "foo EQ 02 00 00 00");

  std::string field_name3 = "foo";
  int val3 = 4;
  QueryCondition query_condition3;
  REQUIRE(query_condition3
              .init(
                  std::string(field_name3),
                  &val3,
                  sizeof(int),
                  QueryConditionOp::EQ)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition3.ast()) ==
      "foo EQ 04 00 00 00");

  std::string field_name4 = "foo";
  int val4 = 6;
  QueryCondition query_condition4;
  REQUIRE(query_condition4
              .init(
                  std::string(field_name4),
                  &val4,
                  sizeof(int),
                  QueryConditionOp::EQ)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition4.ast()) ==
      "foo EQ 06 00 00 00");

  std::string field_name5 = "foo";
  int val5 = 8;
  QueryCondition query_condition5;
  REQUIRE(query_condition5
              .init(
                  std::string(field_name5),
                  &val5,
                  sizeof(int),
                  QueryConditionOp::EQ)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition5.ast()) ==
      "foo EQ 08 00 00 00");

  QueryCondition combined_or1;
  REQUIRE(
      query_condition1
          .combine(
              query_condition2, QueryConditionCombinationOp::OR, &combined_or1)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_or1.ast()) ==
      "(foo EQ 00 00 00 00 OR foo EQ 02 00 00 00)");
  QueryCondition combined_or2;
  REQUIRE(
      combined_or1
          .combine(
              query_condition3, QueryConditionCombinationOp::OR, &combined_or2)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_or2.ast()) ==
      "(foo EQ 00 00 00 00 OR foo EQ 02 00 00 00 OR foo EQ 04 00 00 00)");
  QueryCondition combined_or3;
  REQUIRE(
      combined_or2
          .combine(
              query_condition4, QueryConditionCombinationOp::OR, &combined_or3)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_or3.ast()) ==
      "(foo EQ 00 00 00 00 OR foo EQ 02 00 00 00 OR foo EQ 04 00 00 00 OR foo "
      "EQ 06 00 00 00)");
  QueryCondition combined_or4;
  REQUIRE(
      combined_or3
          .combine(
              query_condition5, QueryConditionCombinationOp::OR, &combined_or4)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(combined_or4.ast()) ==
      "(foo EQ 00 00 00 00 OR foo EQ 02 00 00 00 OR foo EQ 04 00 00 00 OR foo "
      "EQ 06 00 00 00 OR foo "
      "EQ 08 00 00 00)");
}

TEST_CASE(
    "Query Condition: Test AST construction, complex tree with depth > 2",
    "[QueryCondition][ast][api]") {
  std::vector<int> vals = {1, 2, 3, 4, 5, 6, 7, 8, 9};
  std::vector<QueryCondition> qc_value_vector;
  for (int i = 0; i < 7; ++i) {
    QueryCondition qc;
    REQUIRE(qc.init("x", &vals[i], sizeof(vals[i]), QueryConditionOp::EQ).ok());
    REQUIRE(
        tiledb::test::ast_node_to_str(qc.ast()) ==
        "x EQ 0" + std::to_string(vals[i]) + " 00 00 00");
    qc_value_vector.push_back(qc);
  }

  for (int i = 7; i < 9; ++i) {
    QueryCondition qc;
    REQUIRE(qc.init("x", &vals[i], sizeof(vals[i]), QueryConditionOp::NE).ok());
    REQUIRE(
        tiledb::test::ast_node_to_str(qc.ast()) ==
        "x NE 0" + std::to_string(vals[i]) + " 00 00 00");
    qc_value_vector.push_back(qc);
  }

  int x = 6;
  QueryCondition x_neq_six;
  REQUIRE(x_neq_six.init("x", &x, sizeof(x), QueryConditionOp::NE).ok());
  REQUIRE(tiledb::test::ast_node_to_str(x_neq_six.ast()) == "x NE 06 00 00 00");

  QueryCondition one_or_two;
  REQUIRE(
      qc_value_vector[0]
          .combine(
              qc_value_vector[1], QueryConditionCombinationOp::OR, &one_or_two)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(one_or_two.ast()) ==
      "(x EQ 01 00 00 00 OR x EQ 02 00 00 00)");

  QueryCondition three_or_four;
  REQUIRE(qc_value_vector[2]
              .combine(
                  qc_value_vector[3],
                  QueryConditionCombinationOp::OR,
                  &three_or_four)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(three_or_four.ast()) ==
      "(x EQ 03 00 00 00 OR x EQ 04 00 00 00)");

  QueryCondition six_or_seven;
  REQUIRE(qc_value_vector[5]
              .combine(
                  qc_value_vector[6],
                  QueryConditionCombinationOp::OR,
                  &six_or_seven)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(six_or_seven.ast()) ==
      "(x EQ 06 00 00 00 OR x EQ 07 00 00 00)");

  QueryCondition eight_and_nine;
  REQUIRE(qc_value_vector[7]
              .combine(
                  qc_value_vector[8],
                  QueryConditionCombinationOp::AND,
                  &eight_and_nine)
              .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(eight_and_nine.ast()) ==
      "(x NE 08 00 00 00 AND x NE 09 00 00 00)");

  QueryCondition subtree_a;
  REQUIRE(
      one_or_two
          .combine(three_or_four, QueryConditionCombinationOp::AND, &subtree_a)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(subtree_a.ast()) ==
      "((x EQ 01 00 00 00 OR x EQ 02 00 00 00) AND (x EQ 03 00 00 00 OR x EQ "
      "04 00 00 00))");

  QueryCondition subtree_d;
  REQUIRE(
      eight_and_nine
          .combine(six_or_seven, QueryConditionCombinationOp::AND, &subtree_d)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(subtree_d.ast()) ==
      "(x NE 08 00 00 00 AND x NE 09 00 00 00 AND (x EQ 06 00 00 00 OR x EQ 07 "
      "00 00 00))");

  QueryCondition subtree_c;
  REQUIRE(
      subtree_d
          .combine(
              qc_value_vector[4], QueryConditionCombinationOp::OR, &subtree_c)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(subtree_c.ast()) ==
      "((x NE 08 00 00 00 AND x NE 09 00 00 00 AND (x EQ 06 00 00 00 OR x EQ "
      "07 00 00 00)) OR x EQ 05 00 00 00)");

  QueryCondition subtree_b;
  REQUIRE(
      subtree_c.combine(x_neq_six, QueryConditionCombinationOp::AND, &subtree_b)
          .ok());
  REQUIRE(
      tiledb::test::ast_node_to_str(subtree_b.ast()) ==
      "(((x NE 08 00 00 00 AND x NE 09 00 00 00 AND (x EQ 06 00 00 00 OR x EQ "
      "07 00 00 00)) OR x EQ 05 00 00 00) AND x NE 06 00 00 00)");

  QueryCondition qc;
  REQUIRE(
      subtree_a.combine(subtree_b, QueryConditionCombinationOp::OR, &qc).ok());
  REQUIRE(
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
 * @param result_tile The result tile.
 * @param values The values written to the tile.
 */
template <typename T>
void test_apply_cells(
    const QueryConditionOp op,
    const std::string& field_name,
    const uint64_t cells,
    const ArraySchema& array_schema,
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
    const ArraySchema& array_schema,
    ResultTile* const result_tile,
    void* values) {
  const char* const cmp_value = "ae";
  QueryCondition query_condition;
  REQUIRE(query_condition
              .init(std::string(field_name), cmp_value, 2 * sizeof(char), op)
              .ok());
  // Run Check for query_condition
  REQUIRE(query_condition.check(array_schema).ok());

  bool nullable = array_schema.attribute(field_name)->nullable();

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
  REQUIRE(query_condition.apply(array_schema, result_cell_slabs, 1).ok());

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
      REQUIRE(query_condition_eq_null.check(array_schema).ok());

      ResultCellSlab result_cell_slab_eq_null(result_tile, 0, cells);
      std::vector<ResultCellSlab> result_cell_slabs_eq_null;
      result_cell_slabs_eq_null.emplace_back(
          std::move(result_cell_slab_eq_null));
      REQUIRE(query_condition_eq_null
                  .apply(array_schema, result_cell_slabs_eq_null, 1)
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
  const void* fill_value;
  uint64_t fill_value_size;
  array_schema.attribute(field_name)
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
  REQUIRE(query_condition.apply(array_schema, fill_result_cell_slabs, 1).ok());

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
    const ArraySchema& array_schema,
    ResultTile* const result_tile,
    void* values) {
  const T cmp_value = 5;
  QueryCondition query_condition;
  REQUIRE(
      query_condition.init(std::string(field_name), &cmp_value, sizeof(T), op)
          .ok());
  // Run Check for query_condition
  REQUIRE(query_condition.check(array_schema).ok());

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
  REQUIRE(query_condition.apply(array_schema, result_cell_slabs, 1).ok());

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
  const void* fill_value;
  uint64_t fill_value_size;
  array_schema.attribute(field_name)
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
  REQUIRE(query_condition.apply(array_schema, fill_result_cell_slabs, 1).ok());

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
 * @param result_tile The result tile.
 * @param values The values written to the tile.
 */
template <typename T>
void test_apply_operators(
    const std::string& field_name,
    const uint64_t cells,
    const ArraySchema& array_schema,
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
 * @param type The TILEDB data type of the attribute.
 * @param array_schema The array schema.
 * @param result_tile The result tile.
 */
template <typename T>
void test_apply_tile(
    const std::string& field_name,
    uint64_t cells,
    Datatype type,
    const ArraySchema& array_schema,
    ResultTile* result_tile);

/**
 * C-string template-specialization for `test_apply_tile`.
 */
template <>
void test_apply_tile<char*>(
    const std::string& field_name,
    const uint64_t cells,
    const Datatype type,
    const ArraySchema& array_schema,
    ResultTile* const result_tile) {
  ResultTile::TileTuple* const tile_tuple = result_tile->tile_tuple(field_name);

  bool var_size = array_schema.attribute(field_name)->var_size();
  bool nullable = array_schema.attribute(field_name)->nullable();
  Tile* const tile =
      var_size ? &std::get<1>(*tile_tuple) : &std::get<0>(*tile_tuple);

  REQUIRE(tile->init_unfiltered(
                  constants::format_version,
                  type,
                  2 * cells * sizeof(char),
                  2 * sizeof(char),
                  0)
              .ok());

  char* values = static_cast<char*>(malloc(sizeof(char) * 2 * cells));
  for (uint64_t i = 0; i < cells; ++i) {
    values[i * 2] = 'a';
    values[(i * 2) + 1] = 'a' + static_cast<char>(i);
  }
  REQUIRE(tile->write(values, 0, 2 * cells * sizeof(char)).ok());

  if (var_size) {
    Tile* const tile_offsets = &std::get<0>(*tile_tuple);
    REQUIRE(tile_offsets
                ->init_unfiltered(
                    constants::format_version,
                    constants::cell_var_offset_type,
                    10 * constants::cell_var_offset_size,
                    constants::cell_var_offset_size,
                    0)
                .ok());

    uint64_t* offsets =
        static_cast<uint64_t*>(malloc(sizeof(uint64_t) * cells));
    uint64_t offset = 0;
    for (uint64_t i = 0; i < cells; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    REQUIRE(tile_offsets->write(offsets, 0, cells * sizeof(uint64_t)).ok());
  }

  if (nullable) {
    Tile* const tile_validity = &std::get<2>(*tile_tuple);
    REQUIRE(tile_validity
                ->init_unfiltered(
                    constants::format_version,
                    constants::cell_validity_type,
                    10 * constants::cell_validity_size,
                    constants::cell_validity_size,
                    0)
                .ok());

    uint8_t* validity = static_cast<uint8_t*>(malloc(sizeof(uint8_t) * cells));
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE(tile_validity->write(validity, 0, cells * sizeof(uint8_t)).ok());
  }

  test_apply_operators<char*>(
      field_name, cells, array_schema, result_tile, values);

  free(values);
}

/**
 * Non-specialized template type for `test_apply_tile`.
 */
template <typename T>
void test_apply_tile(
    const std::string& field_name,
    const uint64_t cells,
    const Datatype type,
    const ArraySchema& array_schema,
    ResultTile* const result_tile) {
  ResultTile::TileTuple* const tile_tuple = result_tile->tile_tuple(field_name);
  Tile* const tile = &std::get<0>(*tile_tuple);

  REQUIRE(
      tile->init_unfiltered(
              constants::format_version, type, cells * sizeof(T), sizeof(T), 0)
          .ok());
  T* values = static_cast<T*>(malloc(sizeof(T) * cells));
  for (uint64_t i = 0; i < cells; ++i) {
    values[i] = static_cast<T>(i);
  }
  REQUIRE(tile->write(values, 0, cells * sizeof(T)).ok());

  test_apply_operators<T>(field_name, cells, array_schema, result_tile, values);

  free(values);
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
  ArraySchema array_schema;
  Attribute attr(field_name, type);
  REQUIRE(attr.set_nullable(nullable).ok());
  REQUIRE(attr.set_cell_val_num(var_size ? constants::var_num : 2).ok());

  if (!nullable) {
    REQUIRE(attr.set_fill_value(fill_value, 2 * sizeof(char)).ok());
  }

  REQUIRE(
      array_schema.add_attribute(make_shared<Attribute>(HERE(), &attr)).ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(domain.add_dimension(make_shared<Dimension>(HERE(), &dim)).ok());
  REQUIRE(array_schema.set_domain(make_shared<Domain>(HERE(), &domain)).ok());

  // Initialize the result tile.
  ResultTile result_tile(0, 0, array_schema);
  result_tile.init_attr_tile(field_name);

  test_apply_tile<char*>(field_name, cells, type, array_schema, &result_tile);
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
  ArraySchema array_schema;
  Attribute attr(field_name, type);
  REQUIRE(attr.set_cell_val_num(1).ok());
  REQUIRE(attr.set_fill_value(&fill_value, sizeof(T)).ok());
  REQUIRE(
      array_schema.add_attribute(make_shared<Attribute>(HERE(), &attr)).ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(domain.add_dimension(make_shared<Dimension>(HERE(), &dim)).ok());
  REQUIRE(array_schema.set_domain(make_shared<Domain>(HERE(), &domain)).ok());

  // Initialize the result tile.
  ResultTile result_tile(0, 0, array_schema);
  result_tile.init_attr_tile(field_name);

  test_apply_tile<T>(field_name, cells, type, array_schema, &result_tile);
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
    "QueryCondition: Test combinations", "[QueryCondition][combinations]") {
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const Datatype type = Datatype::UINT64;

  // Initialize the array schema.
  ArraySchema array_schema;
  Attribute attr(field_name, type);
  REQUIRE(
      array_schema.add_attribute(make_shared<Attribute>(HERE(), &attr)).ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(domain.add_dimension(make_shared<Dimension>(HERE(), &dim)).ok());
  REQUIRE(array_schema.set_domain(make_shared<Domain>(HERE(), &domain)).ok());

  // Initialize the result tile.
  ResultTile result_tile(0, 0, array_schema);
  result_tile.init_attr_tile(field_name);
  ResultTile::TileTuple* const tile_tuple = result_tile.tile_tuple(field_name);
  Tile* const tile = &std::get<0>(*tile_tuple);

  // Initialize and populate the data tile.
  REQUIRE(tile->init_unfiltered(
                  constants::format_version,
                  type,
                  cells * sizeof(uint64_t),
                  sizeof(uint64_t),
                  0)
              .ok());
  uint64_t* values = static_cast<uint64_t*>(malloc(sizeof(uint64_t) * cells));
  for (uint64_t i = 0; i < cells; ++i) {
    values[i] = i;
  }
  REQUIRE(tile->write(values, 0, cells * sizeof(uint64_t)).ok());

  SECTION("Basic AND condition.") {
    // Build a combined query for `> 3 AND <= 6`.
    uint64_t cmp_value_1 = 3;
    QueryCondition query_condition_1;
    REQUIRE(query_condition_1
                .init(
                    std::string(field_name),
                    &cmp_value_1,
                    sizeof(uint64_t),
                    QueryConditionOp::GT)
                .ok());
    // Run Check for query_condition1
    REQUIRE(query_condition_1.check(array_schema).ok());
    uint64_t cmp_value_2 = 6;
    QueryCondition query_condition_2;
    REQUIRE(query_condition_2
                .init(
                    std::string(field_name),
                    &cmp_value_2,
                    sizeof(uint64_t),
                    QueryConditionOp::LE)
                .ok());
    // Run Check for query_condition2
    REQUIRE(query_condition_2.check(array_schema).ok());
    QueryCondition query_condition_3;
    REQUIRE(query_condition_1
                .combine(
                    query_condition_2,
                    QueryConditionCombinationOp::AND,
                    &query_condition_3)
                .ok());

    ResultCellSlab result_cell_slab(&result_tile, 0, cells);
    std::vector<ResultCellSlab> result_cell_slabs;
    result_cell_slabs.emplace_back(std::move(result_cell_slab));

    REQUIRE(query_condition_3.apply(array_schema, result_cell_slabs, 1).ok());

    // Check that the cell slab now contains cell indexes 4, 5, and 6.
    REQUIRE(result_cell_slabs.size() == 1);
    REQUIRE(result_cell_slabs[0].start_ == 4);
    REQUIRE(result_cell_slabs[0].length_ == 3);
  }

  SECTION("Basic OR condition.") {
    // Build a combined query for `> 6 OR <= 3`.
    uint64_t cmp_value_1 = 6;
    QueryCondition query_condition_1;
    REQUIRE(query_condition_1
                .init(
                    std::string(field_name),
                    &cmp_value_1,
                    sizeof(uint64_t),
                    QueryConditionOp::GT)
                .ok());
    // Run Check for query_condition1
    REQUIRE(query_condition_1.check(array_schema).ok());
    uint64_t cmp_value_2 = 3;
    QueryCondition query_condition_2;
    REQUIRE(query_condition_2
                .init(
                    std::string(field_name),
                    &cmp_value_2,
                    sizeof(uint64_t),
                    QueryConditionOp::LE)
                .ok());
    // Run Check for query_condition2
    REQUIRE(query_condition_2.check(array_schema).ok());
    QueryCondition query_condition_3;
    REQUIRE(query_condition_1
                .combine(
                    query_condition_2,
                    QueryConditionCombinationOp::OR,
                    &query_condition_3)
                .ok());

    ResultCellSlab result_cell_slab(&result_tile, 0, cells);
    std::vector<ResultCellSlab> result_cell_slabs;
    result_cell_slabs.emplace_back(std::move(result_cell_slab));

    REQUIRE(query_condition_3.apply(array_schema, result_cell_slabs, 1).ok());

    // Check that the cell slab now contains cell indexes 0, 1, 2, 3, and 7, 8,
    // 9
    REQUIRE(result_cell_slabs.size() == 2);
    REQUIRE(result_cell_slabs[0].start_ == 0);
    REQUIRE(result_cell_slabs[0].length_ == 4);
    REQUIRE(result_cell_slabs[1].start_ == 7);
    REQUIRE(result_cell_slabs[1].length_ == 3);
  }

  SECTION("OR of 2 AND ASTs.") {
    // Build a combined query for `(>= 3 AND <= 6) OR (> 5 AND < 9)`.
    uint64_t cmp_value_1 = 3;
    QueryCondition query_condition_1;
    REQUIRE(query_condition_1
                .init(
                    std::string(field_name),
                    &cmp_value_1,
                    sizeof(uint64_t),
                    QueryConditionOp::GE)
                .ok());
    // Run Check for query_condition1
    REQUIRE(query_condition_1.check(array_schema).ok());
    uint64_t cmp_value_2 = 6;
    QueryCondition query_condition_2;
    REQUIRE(query_condition_2
                .init(
                    std::string(field_name),
                    &cmp_value_2,
                    sizeof(uint64_t),
                    QueryConditionOp::LE)
                .ok());
    // Run Check for query_condition2
    REQUIRE(query_condition_2.check(array_schema).ok());

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
    // Run Check for query_condition3
    REQUIRE(query_condition_3.check(array_schema).ok());
    uint64_t cmp_value_4 = 9;
    QueryCondition query_condition_4;
    REQUIRE(query_condition_4
                .init(
                    std::string(field_name),
                    &cmp_value_4,
                    sizeof(uint64_t),
                    QueryConditionOp::LT)
                .ok());
    // Run Check for query_condition2
    REQUIRE(query_condition_4.check(array_schema).ok());
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

    ResultCellSlab result_cell_slab(&result_tile, 0, cells);
    std::vector<ResultCellSlab> result_cell_slabs;
    result_cell_slabs.emplace_back(std::move(result_cell_slab));

    REQUIRE(combined_or.apply(array_schema, result_cell_slabs, 1).ok());

    // Check that the cell slab now contains cell indexes 3, 4, 5, 6, 7, 8.
    REQUIRE(result_cell_slabs.size() == 1);
    REQUIRE(result_cell_slabs[0].start_ == 3);
    REQUIRE(result_cell_slabs[0].length_ == 6);
  }

  SECTION("AND of 2 OR ASTs.") {
    // Build a combined query for `(< 3 OR >= 8) AND (<= 4 OR = 9)`.
    uint64_t cmp_value_1 = 3;
    QueryCondition query_condition_1;
    REQUIRE(query_condition_1
                .init(
                    std::string(field_name),
                    &cmp_value_1,
                    sizeof(uint64_t),
                    QueryConditionOp::LT)
                .ok());
    // Run Check for query_condition1
    REQUIRE(query_condition_1.check(array_schema).ok());
    uint64_t cmp_value_2 = 8;
    QueryCondition query_condition_2;
    REQUIRE(query_condition_2
                .init(
                    std::string(field_name),
                    &cmp_value_2,
                    sizeof(uint64_t),
                    QueryConditionOp::GE)
                .ok());
    // Run Check for query_condition2
    REQUIRE(query_condition_2.check(array_schema).ok());

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
    // Run Check for query_condition3
    REQUIRE(query_condition_3.check(array_schema).ok());
    uint64_t cmp_value_4 = 9;
    QueryCondition query_condition_4;
    REQUIRE(query_condition_4
                .init(
                    std::string(field_name),
                    &cmp_value_4,
                    sizeof(uint64_t),
                    QueryConditionOp::EQ)
                .ok());
    // Run Check for query_condition2
    REQUIRE(query_condition_4.check(array_schema).ok());
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

    ResultCellSlab result_cell_slab(&result_tile, 0, cells);
    std::vector<ResultCellSlab> result_cell_slabs;
    result_cell_slabs.emplace_back(std::move(result_cell_slab));

    REQUIRE(combined_and.apply(array_schema, result_cell_slabs, 1).ok());

    // Check that the cell slab now contains cell indexes 0, 1, 2, 9.
    REQUIRE(result_cell_slabs.size() == 2);
    REQUIRE(result_cell_slabs[0].start_ == 0);
    REQUIRE(result_cell_slabs[0].length_ == 3);
    REQUIRE(result_cell_slabs[1].start_ == 9);
    REQUIRE(result_cell_slabs[1].length_ == 1);
  }

  SECTION("Complex tree with depth > 2.") {
    // Build a combined query for (((x = 1 || x = 2) && (x = 3 || x = 4)) ||
    // (((x
    // != 8 && x != 9 && (x = 6 || x = 7)) || x = 5) && x != 6))
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

    ResultCellSlab result_cell_slab(&result_tile, 0, cells);
    std::vector<ResultCellSlab> result_cell_slabs;
    result_cell_slabs.emplace_back(std::move(result_cell_slab));

    REQUIRE(qc.apply(array_schema, result_cell_slabs, 1).ok());

    // Check that the cell slab now contains cell indexes 5, 7.
    REQUIRE(result_cell_slabs.size() == 2);
    REQUIRE(result_cell_slabs[0].start_ == 5);
    REQUIRE(result_cell_slabs[0].length_ == 1);
    REQUIRE(result_cell_slabs[1].start_ == 7);
    REQUIRE(result_cell_slabs[1].length_ == 1);
  }

  SECTION("Adding simple clauses to AND tree.") {
    // foo != 1 && foo != 3 && foo != 5 && foo != 7 && foo != 9
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

    ResultCellSlab result_cell_slab(&result_tile, 0, cells);
    std::vector<ResultCellSlab> result_cell_slabs;
    result_cell_slabs.emplace_back(std::move(result_cell_slab));

    REQUIRE(combined_and4.apply(array_schema, result_cell_slabs, 1).ok());

    // Check that the cell slab now contains cell indexes 0, 2, 4, 6, 8.
    REQUIRE(result_cell_slabs.size() == 5);
    REQUIRE(result_cell_slabs[0].start_ == 0);
    REQUIRE(result_cell_slabs[0].length_ == 1);
    REQUIRE(result_cell_slabs[1].start_ == 2);
    REQUIRE(result_cell_slabs[1].length_ == 1);
    REQUIRE(result_cell_slabs[2].start_ == 4);
    REQUIRE(result_cell_slabs[2].length_ == 1);
    REQUIRE(result_cell_slabs[3].start_ == 6);
    REQUIRE(result_cell_slabs[3].length_ == 1);
    REQUIRE(result_cell_slabs[4].start_ == 8);
    REQUIRE(result_cell_slabs[4].length_ == 1);
  }

  SECTION("Adding simple clauses to OR tree.") {
    // foo = 0 || foo = 2 || foo = 4 || foo = 6 || foo = 8
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

    ResultCellSlab result_cell_slab(&result_tile, 0, cells);
    std::vector<ResultCellSlab> result_cell_slabs;
    result_cell_slabs.emplace_back(std::move(result_cell_slab));

    REQUIRE(combined_or4.apply(array_schema, result_cell_slabs, 1).ok());

    // Check that the cell slab now contains cell indexes 0, 2, 4, 6, 8.
    REQUIRE(result_cell_slabs.size() == 5);
    REQUIRE(result_cell_slabs[0].start_ == 0);
    REQUIRE(result_cell_slabs[0].length_ == 1);
    REQUIRE(result_cell_slabs[1].start_ == 2);
    REQUIRE(result_cell_slabs[1].length_ == 1);
    REQUIRE(result_cell_slabs[2].start_ == 4);
    REQUIRE(result_cell_slabs[2].length_ == 1);
    REQUIRE(result_cell_slabs[3].start_ == 6);
    REQUIRE(result_cell_slabs[3].length_ == 1);
    REQUIRE(result_cell_slabs[4].start_ == 8);
    REQUIRE(result_cell_slabs[4].length_ == 1);
  }

  free(values);
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
  ArraySchema array_schema;
  Attribute attr(field_name, type);
  REQUIRE(attr.set_nullable(nullable).ok());
  REQUIRE(attr.set_cell_val_num(var_size ? constants::var_num : 2).ok());

  if (!nullable) {
    REQUIRE(attr.set_fill_value(fill_value, 2 * sizeof(char)).ok());
  }

  REQUIRE(array_schema.add_attribute(tdb::make_shared<Attribute>(HERE(), &attr))
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
      array_schema.set_domain(make_shared<tiledb::sm::Domain>(HERE(), &domain))
          .ok());

  // Initialize the result tile.
  ResultTile result_tile(0, 0, array_schema);
  result_tile.init_attr_tile(field_name);

  ResultTile::TileTuple* const tile_tuple = result_tile.tile_tuple(field_name);

  var_size = array_schema.attribute(field_name)->var_size();
  nullable = array_schema.attribute(field_name)->nullable();
  Tile* const tile =
      var_size ? &std::get<1>(*tile_tuple) : &std::get<0>(*tile_tuple);

  REQUIRE(tile->init_unfiltered(
                  constants::format_version,
                  type,
                  2 * (cells - 2) * sizeof(char),
                  2 * sizeof(char),
                  0)
              .ok());

  char* values = static_cast<char*>(malloc(
      sizeof(char) * 2 *
      (cells - 2)));  // static_cast<char*>(malloc(sizeof(char) * 2 * cells));

  // Empty strings are at idx 8 and 9
  for (uint64_t i = 0; i < (cells - 2); ++i) {
    values[i * 2] = 'a';
    values[(i * 2) + 1] = 'a' + static_cast<char>(i);
  }

  REQUIRE(tile->write(values, 0, 2 * (cells - 2) * sizeof(char)).ok());

  if (var_size) {
    Tile* const tile_offsets = &std::get<0>(*tile_tuple);
    REQUIRE(tile_offsets
                ->init_unfiltered(
                    constants::format_version,
                    constants::cell_var_offset_type,
                    10 * constants::cell_var_offset_size,
                    constants::cell_var_offset_size,
                    0)
                .ok());

    uint64_t* offsets =
        static_cast<uint64_t*>(malloc(sizeof(uint64_t) * cells));
    uint64_t offset = 0;
    for (uint64_t i = 0; i < cells - 2; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    offsets[cells - 2] = offset;
    offsets[cells - 1] = offset;
    REQUIRE(tile_offsets->write(offsets, 0, cells * sizeof(uint64_t)).ok());

    free(offsets);
  }

  if (nullable) {
    Tile* const tile_validity = &std::get<2>(*tile_tuple);
    REQUIRE(tile_validity
                ->init_unfiltered(
                    constants::format_version,
                    constants::cell_validity_type,
                    10 * constants::cell_validity_size,
                    constants::cell_validity_size,
                    0)
                .ok());

    uint8_t* validity = static_cast<uint8_t*>(malloc(sizeof(uint8_t) * cells));
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE(tile_validity->write(validity, 0, cells * sizeof(uint8_t)).ok());

    free(validity);
  }

  // Empty string or null string as condition value
  const char* cmp_value = null_cmp ? nullptr : "";

  QueryCondition query_condition;
  REQUIRE(query_condition.init(std::string(field_name), cmp_value, 0, op).ok());

  // Run Check for query_condition
  REQUIRE(query_condition.check(array_schema).ok());

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
  REQUIRE(query_condition.apply(array_schema, result_cell_slabs, 1).ok());

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

  free(values);
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
    const ArraySchema& array_schema,
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
    const ArraySchema& array_schema,
    ResultTile* const result_tile,
    void* values) {
  const char* const cmp_value = "ae";
  QueryCondition query_condition;
  REQUIRE(query_condition
              .init(std::string(field_name), cmp_value, 2 * sizeof(char), op)
              .ok());
  // Run Check for query_condition
  REQUIRE(query_condition.check(array_schema).ok());

  bool nullable = array_schema.attribute(field_name)->nullable();

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
                  array_schema, result_tile, 0, 10, 0, 1, result_bitmap.data())
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
      REQUIRE(query_condition_eq_null.check(array_schema).ok());

      // Apply the query condition.
      std::vector<uint8_t> result_bitmap_eq_null(cells, 1);
      REQUIRE(query_condition_eq_null
                  .apply_dense(
                      array_schema,
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
    const ArraySchema& array_schema,
    ResultTile* const result_tile,
    void* values) {
  const T cmp_value = 5;
  QueryCondition query_condition;
  REQUIRE(
      query_condition.init(std::string(field_name), &cmp_value, sizeof(T), op)
          .ok());
  // Run Check for query_condition
  REQUIRE(query_condition.check(array_schema).ok());

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
                  array_schema, result_tile, 0, 10, 0, 1, result_bitmap.data())
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
 * @param result_tile The result tile.
 * @param values The values written to the tile.
 */
template <typename T>
void test_apply_operators_dense(
    const std::string& field_name,
    const uint64_t cells,
    const ArraySchema& array_schema,
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
 * @param type The TILEDB data type of the attribute.
 * @param array_schema The array schema.
 * @param result_tile The result tile.
 */
template <typename T>
void test_apply_tile_dense(
    const std::string& field_name,
    uint64_t cells,
    Datatype type,
    const ArraySchema& array_schema,
    ResultTile* result_tile);

/**
 * C-string template-specialization for `test_apply_tile_dense`.
 */
template <>
void test_apply_tile_dense<char*>(
    const std::string& field_name,
    const uint64_t cells,
    const Datatype type,
    const ArraySchema& array_schema,
    ResultTile* const result_tile) {
  ResultTile::TileTuple* const tile_tuple = result_tile->tile_tuple(field_name);

  bool var_size = array_schema.attribute(field_name)->var_size();
  bool nullable = array_schema.attribute(field_name)->nullable();
  Tile* const tile =
      var_size ? &std::get<1>(*tile_tuple) : &std::get<0>(*tile_tuple);

  REQUIRE(tile->init_unfiltered(
                  constants::format_version,
                  type,
                  2 * cells * sizeof(char),
                  2 * sizeof(char),
                  0)
              .ok());

  char* values = static_cast<char*>(malloc(sizeof(char) * 2 * cells));
  for (uint64_t i = 0; i < cells; ++i) {
    values[i * 2] = 'a';
    values[(i * 2) + 1] = 'a' + static_cast<char>(i);
  }
  REQUIRE(tile->write(values, 0, 2 * cells * sizeof(char)).ok());

  if (var_size) {
    Tile* const tile_offsets = &std::get<0>(*tile_tuple);
    REQUIRE(tile_offsets
                ->init_unfiltered(
                    constants::format_version,
                    constants::cell_var_offset_type,
                    10 * constants::cell_var_offset_size,
                    constants::cell_var_offset_size,
                    0)
                .ok());

    uint64_t* offsets =
        static_cast<uint64_t*>(malloc(sizeof(uint64_t) * cells));
    uint64_t offset = 0;
    for (uint64_t i = 0; i < cells; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    REQUIRE(tile_offsets->write(offsets, 0, cells * sizeof(uint64_t)).ok());
  }

  if (nullable) {
    Tile* const tile_validity = &std::get<2>(*tile_tuple);
    REQUIRE(tile_validity
                ->init_unfiltered(
                    constants::format_version,
                    constants::cell_validity_type,
                    10 * constants::cell_validity_size,
                    constants::cell_validity_size,
                    0)
                .ok());

    uint8_t* validity = static_cast<uint8_t*>(malloc(sizeof(uint8_t) * cells));
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE(tile_validity->write(validity, 0, cells * sizeof(uint8_t)).ok());
  }

  test_apply_operators_dense<char*>(
      field_name, cells, array_schema, result_tile, values);

  free(values);
}

/**
 * Non-specialized template type for `test_apply_tile_dense`.
 */
template <typename T>
void test_apply_tile_dense(
    const std::string& field_name,
    const uint64_t cells,
    const Datatype type,
    const ArraySchema& array_schema,
    ResultTile* const result_tile) {
  ResultTile::TileTuple* const tile_tuple = result_tile->tile_tuple(field_name);
  Tile* const tile = &std::get<0>(*tile_tuple);

  REQUIRE(
      tile->init_unfiltered(
              constants::format_version, type, cells * sizeof(T), sizeof(T), 0)
          .ok());
  T* values = static_cast<T*>(malloc(sizeof(T) * cells));
  for (uint64_t i = 0; i < cells; ++i) {
    values[i] = static_cast<T>(i);
  }
  REQUIRE(tile->write(values, 0, cells * sizeof(T)).ok());

  test_apply_operators_dense<T>(
      field_name, cells, array_schema, result_tile, values);

  free(values);
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
  ArraySchema array_schema;
  Attribute attr(field_name, type);
  REQUIRE(attr.set_nullable(nullable).ok());
  REQUIRE(attr.set_cell_val_num(var_size ? constants::var_num : 2).ok());

  if (!nullable) {
    REQUIRE(attr.set_fill_value(fill_value, 2 * sizeof(char)).ok());
  }

  REQUIRE(
      array_schema.add_attribute(make_shared<Attribute>(HERE(), &attr)).ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(domain.add_dimension(make_shared<Dimension>(HERE(), &dim)).ok());
  REQUIRE(array_schema.set_domain(make_shared<Domain>(HERE(), &domain)).ok());

  // Initialize the result tile.
  ResultTile result_tile(0, 0, array_schema);
  result_tile.init_attr_tile(field_name);

  test_apply_tile_dense<char*>(
      field_name, cells, type, array_schema, &result_tile);
}

/**
 * Non-specialized template type for `test_apply_dense`.
 */
template <typename T>
void test_apply_dense(const Datatype type, bool var_size, bool nullable) {
  (void)var_size;
  (void)nullable;
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const T fill_value = 3;

  // Initialize the array schema.
  ArraySchema array_schema;
  Attribute attr(field_name, type);
  REQUIRE(attr.set_cell_val_num(1).ok());
  REQUIRE(attr.set_fill_value(&fill_value, sizeof(T)).ok());
  REQUIRE(
      array_schema.add_attribute(make_shared<Attribute>(HERE(), &attr)).ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(domain.add_dimension(make_shared<Dimension>(HERE(), &dim)).ok());
  REQUIRE(array_schema.set_domain(make_shared<Domain>(HERE(), &domain)).ok());

  // Initialize the result tile.
  ResultTile result_tile(0, 0, array_schema);
  result_tile.init_attr_tile(field_name);

  test_apply_tile_dense<T>(field_name, cells, type, array_schema, &result_tile);
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
    "QueryCondition: Test combinations dense",
    "[QueryCondition][combinations][dense]") {
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const Datatype type = Datatype::UINT64;

  // Initialize the array schema.
  ArraySchema array_schema;
  Attribute attr(field_name, type);
  REQUIRE(
      array_schema.add_attribute(make_shared<Attribute>(HERE(), &attr)).ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(domain.add_dimension(make_shared<Dimension>(HERE(), &dim)).ok());
  REQUIRE(array_schema.set_domain(make_shared<Domain>(HERE(), &domain)).ok());

  // Initialize the result tile.
  ResultTile result_tile(0, 0, array_schema);
  result_tile.init_attr_tile(field_name);
  ResultTile::TileTuple* const tile_tuple = result_tile.tile_tuple(field_name);
  Tile* const tile = &std::get<0>(*tile_tuple);

  // Initialize and populate the data tile.
  REQUIRE(tile->init_unfiltered(
                  constants::format_version,
                  type,
                  cells * sizeof(uint64_t),
                  sizeof(uint64_t),
                  0)
              .ok());
  uint64_t* values = static_cast<uint64_t*>(malloc(sizeof(uint64_t) * cells));
  for (uint64_t i = 0; i < cells; ++i) {
    values[i] = i;
  }
  REQUIRE(tile->write(values, 0, cells * sizeof(uint64_t)).ok());

  SECTION("Basic AND condition.") {
    // Build a combined query for `> 3 AND <= 6`.
    uint64_t cmp_value_1 = 3;
    QueryCondition query_condition_1;
    REQUIRE(query_condition_1
                .init(
                    std::string(field_name),
                    &cmp_value_1,
                    sizeof(uint64_t),
                    QueryConditionOp::GT)
                .ok());
    // Run Check for query_condition1
    REQUIRE(query_condition_1.check(array_schema).ok());
    uint64_t cmp_value_2 = 6;
    QueryCondition query_condition_2;
    REQUIRE(query_condition_2
                .init(
                    std::string(field_name),
                    &cmp_value_2,
                    sizeof(uint64_t),
                    QueryConditionOp::LE)
                .ok());
    // Run Check for query_condition2
    REQUIRE(query_condition_2.check(array_schema).ok());
    QueryCondition query_condition_3;
    REQUIRE(query_condition_1
                .combine(
                    query_condition_2,
                    QueryConditionCombinationOp::AND,
                    &query_condition_3)
                .ok());

    // Apply the query condition.
    std::vector<uint8_t> result_bitmap(cells, 1);
    REQUIRE(
        query_condition_3
            .apply_dense(
                array_schema, &result_tile, 0, 10, 0, 1, result_bitmap.data())
            .ok());

    // Check that the cell slab now contains cell indexes 4, 5, and 6.
    for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
      REQUIRE(
          result_bitmap[cell_idx] == (cell_idx >= 4 && cell_idx <= 6 ? 1 : 0));
    }
  }

  SECTION("Basic OR condition.") {
    // Build a combined query for `> 6 OR <= 3`.
    uint64_t cmp_value_1 = 6;
    QueryCondition query_condition_1;
    REQUIRE(query_condition_1
                .init(
                    std::string(field_name),
                    &cmp_value_1,
                    sizeof(uint64_t),
                    QueryConditionOp::GT)
                .ok());
    // Run Check for query_condition1
    REQUIRE(query_condition_1.check(array_schema).ok());
    uint64_t cmp_value_2 = 3;
    QueryCondition query_condition_2;
    REQUIRE(query_condition_2
                .init(
                    std::string(field_name),
                    &cmp_value_2,
                    sizeof(uint64_t),
                    QueryConditionOp::LE)
                .ok());
    // Run Check for query_condition2
    REQUIRE(query_condition_2.check(array_schema).ok());
    QueryCondition query_condition_3;
    REQUIRE(query_condition_1
                .combine(
                    query_condition_2,
                    QueryConditionCombinationOp::OR,
                    &query_condition_3)
                .ok());

    // Apply the query condition.
    std::vector<uint8_t> result_bitmap(cells, 1);
    REQUIRE(
        query_condition_3
            .apply_dense(
                array_schema, &result_tile, 0, 10, 0, 1, result_bitmap.data())
            .ok());

    // Check that the cell slab now contains cell indexes 0, 1, 2, 3, and 7,
    // 8, 9.
    for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
      REQUIRE(
          result_bitmap[cell_idx] == (cell_idx >= 7 || cell_idx <= 3 ? 1 : 0));
    }
  }

  SECTION("OR of 2 AND ASTs.") {
    // Build a combined query for `(>= 3 AND <= 6) OR (> 5 AND < 9)`.
    uint64_t cmp_value_1 = 3;
    QueryCondition query_condition_1;
    REQUIRE(query_condition_1
                .init(
                    std::string(field_name),
                    &cmp_value_1,
                    sizeof(uint64_t),
                    QueryConditionOp::GE)
                .ok());
    // Run Check for query_condition1
    REQUIRE(query_condition_1.check(array_schema).ok());
    uint64_t cmp_value_2 = 6;
    QueryCondition query_condition_2;
    REQUIRE(query_condition_2
                .init(
                    std::string(field_name),
                    &cmp_value_2,
                    sizeof(uint64_t),
                    QueryConditionOp::LE)
                .ok());
    // Run Check for query_condition2
    REQUIRE(query_condition_2.check(array_schema).ok());

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
    // Run Check for query_condition3
    REQUIRE(query_condition_3.check(array_schema).ok());
    uint64_t cmp_value_4 = 9;
    QueryCondition query_condition_4;
    REQUIRE(query_condition_4
                .init(
                    std::string(field_name),
                    &cmp_value_4,
                    sizeof(uint64_t),
                    QueryConditionOp::LT)
                .ok());
    // Run Check for query_condition2
    REQUIRE(query_condition_4.check(array_schema).ok());
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

    // Apply the query condition.
    std::vector<uint8_t> result_bitmap(cells, 1);
    REQUIRE(
        combined_or
            .apply_dense(
                array_schema, &result_tile, 0, 10, 0, 1, result_bitmap.data())
            .ok());

    // Check that the cell slab now contains cell indexes 3, 4, 5, 6, 7, 8.
    for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
      REQUIRE(
          result_bitmap[cell_idx] == (cell_idx >= 3 && cell_idx <= 8 ? 1 : 0));
    }
  }

  SECTION("AND of 2 OR ASTs.") {
    // Build a combined query for `(< 3 OR >= 8) AND (<= 4 OR = 9)`.
    uint64_t cmp_value_1 = 3;
    QueryCondition query_condition_1;
    REQUIRE(query_condition_1
                .init(
                    std::string(field_name),
                    &cmp_value_1,
                    sizeof(uint64_t),
                    QueryConditionOp::LT)
                .ok());
    // Run Check for query_condition1
    REQUIRE(query_condition_1.check(array_schema).ok());
    uint64_t cmp_value_2 = 8;
    QueryCondition query_condition_2;
    REQUIRE(query_condition_2
                .init(
                    std::string(field_name),
                    &cmp_value_2,
                    sizeof(uint64_t),
                    QueryConditionOp::GE)
                .ok());
    // Run Check for query_condition2
    REQUIRE(query_condition_2.check(array_schema).ok());

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
    // Run Check for query_condition3
    REQUIRE(query_condition_3.check(array_schema).ok());
    uint64_t cmp_value_4 = 9;
    QueryCondition query_condition_4;
    REQUIRE(query_condition_4
                .init(
                    std::string(field_name),
                    &cmp_value_4,
                    sizeof(uint64_t),
                    QueryConditionOp::EQ)
                .ok());
    // Run Check for query_condition2
    REQUIRE(query_condition_4.check(array_schema).ok());
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

    // Apply the query condition.
    std::vector<uint8_t> result_bitmap(cells, 1);
    REQUIRE(
        combined_and
            .apply_dense(
                array_schema, &result_tile, 0, 10, 0, 1, result_bitmap.data())
            .ok());

    // Check that the cell slab now contains cell indexes 0, 1, 2, 3, and 7,
    // 8, 9.
    for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
      REQUIRE(
          result_bitmap[cell_idx] == (cell_idx <= 2 || cell_idx == 9 ? 1 : 0));
    }
  }

  SECTION("Complex tree with depth > 2.") {
    // Build a combined query for (((x = 1 || x = 2) && (x = 3 || x = 4)) ||
    // (((x
    // != 8 && x != 9 && (x = 6 || x = 7)) || x = 5) && x != 6))
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

    // Apply the query condition.
    std::vector<uint8_t> result_bitmap(cells, 1);
    REQUIRE(qc.apply_dense(
                  array_schema, &result_tile, 0, 10, 0, 1, result_bitmap.data())
                .ok());

    // Check that the cell slab now contains cell indexes 5 and 7.
    for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
      REQUIRE(
          result_bitmap[cell_idx] == (cell_idx == 5 || cell_idx == 7 ? 1 : 0));
    }
  }

  SECTION("Adding simple clauses to AND tree.") {
    // foo != 1 && foo != 3 && foo != 5 && foo != 7 && foo != 9
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

    // Apply the query condition.
    std::vector<uint8_t> result_bitmap(cells, 1);
    REQUIRE(
        combined_and4
            .apply_dense(
                array_schema, &result_tile, 0, 10, 0, 1, result_bitmap.data())
            .ok());

    // Check that the cell slab now contains cell indexes 0, 2, 4, 6, 8.
    for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
      REQUIRE(result_bitmap[cell_idx] == (cell_idx % 2 == 0 ? 1 : 0));
    }
  }

  SECTION("Adding simple clauses to OR tree.") {
    // foo = 0 || foo = 2 || foo = 4 || foo = 6 || foo = 8
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

    // Apply the query condition.
    std::vector<uint8_t> result_bitmap(cells, 1);
    REQUIRE(
        combined_or4
            .apply_dense(
                array_schema, &result_tile, 0, 10, 0, 1, result_bitmap.data())
            .ok());

    // Check that the cell slab now contains cell indexes 0, 2, 4, 6, 8.
    for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
      REQUIRE(result_bitmap[cell_idx] == (cell_idx % 2 == 0 ? 1 : 0));
    }
  }

  free(values);
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
  ArraySchema array_schema;
  Attribute attr(field_name, type);
  REQUIRE(attr.set_nullable(nullable).ok());
  REQUIRE(attr.set_cell_val_num(var_size ? constants::var_num : 2).ok());

  if (!nullable) {
    REQUIRE(attr.set_fill_value(fill_value, 2 * sizeof(char)).ok());
  }

  REQUIRE(
      array_schema.add_attribute(make_shared<Attribute>(HERE(), &attr)).ok());
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
      array_schema.set_domain(make_shared<tiledb::sm::Domain>(HERE(), &domain))
          .ok());

  // Initialize the result tile.
  ResultTile result_tile(0, 0, array_schema);
  result_tile.init_attr_tile(field_name);

  ResultTile::TileTuple* const tile_tuple = result_tile.tile_tuple(field_name);

  var_size = array_schema.attribute(field_name)->var_size();
  nullable = array_schema.attribute(field_name)->nullable();
  Tile* const tile =
      var_size ? &std::get<1>(*tile_tuple) : &std::get<0>(*tile_tuple);

  REQUIRE(tile->init_unfiltered(
                  constants::format_version,
                  type,
                  2 * (cells - 2) * sizeof(char),
                  2 * sizeof(char),
                  0)
              .ok());

  char* values = static_cast<char*>(malloc(
      sizeof(char) * 2 *
      (cells - 2)));  // static_cast<char*>(malloc(sizeof(char) * 2 * cells));

  // Empty strings are at idx 8 and 9
  for (uint64_t i = 0; i < (cells - 2); ++i) {
    values[i * 2] = 'a';
    values[(i * 2) + 1] = 'a' + static_cast<char>(i);
  }

  REQUIRE(tile->write(values, 0, 2 * (cells - 2) * sizeof(char)).ok());

  if (var_size) {
    Tile* const tile_offsets = &std::get<0>(*tile_tuple);
    REQUIRE(tile_offsets
                ->init_unfiltered(
                    constants::format_version,
                    constants::cell_var_offset_type,
                    10 * constants::cell_var_offset_size,
                    constants::cell_var_offset_size,
                    0)
                .ok());

    uint64_t* offsets =
        static_cast<uint64_t*>(malloc(sizeof(uint64_t) * cells));
    uint64_t offset = 0;
    for (uint64_t i = 0; i < cells - 2; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    offsets[cells - 2] = offset;
    offsets[cells - 1] = offset;
    REQUIRE(tile_offsets->write(offsets, 0, cells * sizeof(uint64_t)).ok());

    free(offsets);
  }

  if (nullable) {
    Tile* const tile_validity = &std::get<2>(*tile_tuple);
    REQUIRE(tile_validity
                ->init_unfiltered(
                    constants::format_version,
                    constants::cell_validity_type,
                    10 * constants::cell_validity_size,
                    constants::cell_validity_size,
                    0)
                .ok());

    uint8_t* validity = static_cast<uint8_t*>(malloc(sizeof(uint8_t) * cells));
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE(tile_validity->write(validity, 0, cells * sizeof(uint8_t)).ok());

    free(validity);
  }

  // Empty string or null string as condition value
  const char* cmp_value = null_cmp ? nullptr : "";

  QueryCondition query_condition;
  REQUIRE(query_condition.init(std::string(field_name), cmp_value, 0, op).ok());

  // Run Check for query_condition
  REQUIRE(query_condition.check(array_schema).ok());

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
              .apply_dense(
                  array_schema, &result_tile, 0, 10, 0, 1, result_bitmap.data())
              .ok());

  // Verify the result bitmap contain the expected cells.
  auto expected_iter = expected_cell_idx_vec.begin();
  for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
    if (result_bitmap[cell_idx]) {
      REQUIRE(*expected_iter == cell_idx);
      ++expected_iter;
    }
  }

  free(values);
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
void test_apply_cells_sparse(
    const QueryConditionOp op,
    const std::string& field_name,
    const uint64_t cells,
    const ArraySchema& array_schema,
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
    const ArraySchema& array_schema,
    ResultTile* const result_tile,
    void* values) {
  const char* const cmp_value = "ae";
  QueryCondition query_condition;
  REQUIRE(query_condition
              .init(std::string(field_name), cmp_value, 2 * sizeof(char), op)
              .ok());
  // Run Check for query_condition
  REQUIRE(query_condition.check(array_schema).ok());

  bool nullable = array_schema.attribute(field_name)->nullable();

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
  uint64_t cell_count = 0;
  std::vector<uint8_t> result_bitmap(cells, 1);
  REQUIRE(query_condition
              .apply_sparse<uint8_t>(
                  array_schema, *result_tile, result_bitmap, &cell_count)
              .ok());

  // Verify the result bitmap contain the expected cells.
  REQUIRE(cell_count == expected_cell_idx_vec.size());
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
      REQUIRE(query_condition_eq_null.check(array_schema).ok());

      // Apply the query condition.
      uint64_t cell_count_eq_null = 0;
      std::vector<uint8_t> result_bitmap_eq_null(cells, 1);
      REQUIRE(query_condition_eq_null
                  .apply_sparse<uint8_t>(
                      array_schema,
                      *result_tile,
                      result_bitmap_eq_null,
                      &cell_count_eq_null)
                  .ok());

      // Verify the result bitmap contain the expected cells.
      REQUIRE(cell_count_eq_null == 5);
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
    const ArraySchema& array_schema,
    ResultTile* const result_tile,
    void* values) {
  const T cmp_value = 5;
  QueryCondition query_condition;
  REQUIRE(
      query_condition.init(std::string(field_name), &cmp_value, sizeof(T), op)
          .ok());
  // Run Check for query_condition
  REQUIRE(query_condition.check(array_schema).ok());

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
  uint64_t cell_count = 0;
  std::vector<uint8_t> result_bitmap(cells, 1);
  REQUIRE(query_condition
              .apply_sparse<uint8_t>(
                  array_schema, *result_tile, result_bitmap, &cell_count)
              .ok());

  // Verify the result bitmap contain the expected cells.
  REQUIRE(cell_count == expected_cell_idx_vec.size());
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
 * @param result_tile The result tile.
 * @param values The values written to the tile.
 */
template <typename T>
void test_apply_operators_sparse(
    const std::string& field_name,
    const uint64_t cells,
    const ArraySchema& array_schema,
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
 * @param type The TILEDB data type of the attribute.
 * @param array_schema The array schema.
 * @param result_tile The result tile.
 */
template <typename T>
void test_apply_tile_sparse(
    const std::string& field_name,
    uint64_t cells,
    Datatype type,
    const ArraySchema& array_schema,
    ResultTile* result_tile);

/**
 * C-string template-specialization for `test_apply_tile_sparse`.
 */
template <>
void test_apply_tile_sparse<char*>(
    const std::string& field_name,
    const uint64_t cells,
    const Datatype type,
    const ArraySchema& array_schema,
    ResultTile* const result_tile) {
  ResultTile::TileTuple* const tile_tuple = result_tile->tile_tuple(field_name);

  bool var_size = array_schema.attribute(field_name)->var_size();
  bool nullable = array_schema.attribute(field_name)->nullable();
  Tile* const tile =
      var_size ? &std::get<1>(*tile_tuple) : &std::get<0>(*tile_tuple);

  REQUIRE(tile->init_unfiltered(
                  constants::format_version,
                  type,
                  2 * cells * sizeof(char),
                  2 * sizeof(char),
                  0)
              .ok());

  char* values = static_cast<char*>(malloc(sizeof(char) * 2 * cells));
  for (uint64_t i = 0; i < cells; ++i) {
    values[i * 2] = 'a';
    values[(i * 2) + 1] = 'a' + static_cast<char>(i);
  }
  REQUIRE(tile->write(values, 0, 2 * cells * sizeof(char)).ok());

  if (var_size) {
    Tile* const tile_offsets = &std::get<0>(*tile_tuple);
    REQUIRE(tile_offsets
                ->init_unfiltered(
                    constants::format_version,
                    constants::cell_var_offset_type,
                    10 * constants::cell_var_offset_size,
                    constants::cell_var_offset_size,
                    0)
                .ok());

    uint64_t* offsets =
        static_cast<uint64_t*>(malloc(sizeof(uint64_t) * cells));
    uint64_t offset = 0;
    for (uint64_t i = 0; i < cells; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    REQUIRE(tile_offsets->write(offsets, 0, cells * sizeof(uint64_t)).ok());
  }

  if (nullable) {
    Tile* const tile_validity = &std::get<2>(*tile_tuple);
    REQUIRE(tile_validity
                ->init_unfiltered(
                    constants::format_version,
                    constants::cell_validity_type,
                    10 * constants::cell_validity_size,
                    constants::cell_validity_size,
                    0)
                .ok());

    uint8_t* validity = static_cast<uint8_t*>(malloc(sizeof(uint8_t) * cells));
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE(tile_validity->write(validity, 0, cells * sizeof(uint8_t)).ok());
  }

  test_apply_operators_sparse<char*>(
      field_name, cells, array_schema, result_tile, values);

  free(values);
}

/**
 * Non-specialized template type for `test_apply_tile_sparse`.
 */
template <typename T>
void test_apply_tile_sparse(
    const std::string& field_name,
    const uint64_t cells,
    const Datatype type,
    const ArraySchema& array_schema,
    ResultTile* const result_tile) {
  ResultTile::TileTuple* const tile_tuple = result_tile->tile_tuple(field_name);
  Tile* const tile = &std::get<0>(*tile_tuple);

  REQUIRE(
      tile->init_unfiltered(
              constants::format_version, type, cells * sizeof(T), sizeof(T), 0)
          .ok());
  T* values = static_cast<T*>(malloc(sizeof(T) * cells));
  for (uint64_t i = 0; i < cells; ++i) {
    values[i] = static_cast<T>(i);
  }
  REQUIRE(tile->write(values, 0, cells * sizeof(T)).ok());

  test_apply_operators_sparse<T>(
      field_name, cells, array_schema, result_tile, values);

  free(values);
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
  ArraySchema array_schema;
  Attribute attr(field_name, type);
  REQUIRE(attr.set_nullable(nullable).ok());
  REQUIRE(attr.set_cell_val_num(var_size ? constants::var_num : 2).ok());

  if (!nullable) {
    REQUIRE(attr.set_fill_value(fill_value, 2 * sizeof(char)).ok());
  }

  REQUIRE(
      array_schema.add_attribute(make_shared<Attribute>(HERE(), &attr)).ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(domain.add_dimension(make_shared<Dimension>(HERE(), &dim)).ok());
  REQUIRE(array_schema.set_domain(make_shared<Domain>(HERE(), &domain)).ok());

  // Initialize the result tile.
  ResultTile result_tile(0, 0, array_schema);
  result_tile.init_attr_tile(field_name);

  test_apply_tile_sparse<char*>(
      field_name, cells, type, array_schema, &result_tile);
}

/**
 * Non-specialized template type for `test_apply_sparse`.
 */
template <typename T>
void test_apply_sparse(const Datatype type, bool var_size, bool nullable) {
  (void)var_size;
  (void)nullable;
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const T fill_value = 3;

  // Initialize the array schema.
  ArraySchema array_schema;
  Attribute attr(field_name, type);
  REQUIRE(attr.set_cell_val_num(1).ok());
  REQUIRE(attr.set_fill_value(&fill_value, sizeof(T)).ok());
  REQUIRE(array_schema.add_attribute(tdb::make_shared<Attribute>(HERE(), &attr))
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
      array_schema.set_domain(make_shared<tiledb::sm::Domain>(HERE(), &domain))
          .ok());

  // Initialize the result tile.
  ResultTile result_tile(0, 0, array_schema);
  result_tile.init_attr_tile(field_name);

  test_apply_tile_sparse<T>(
      field_name, cells, type, array_schema, &result_tile);
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

TEST_CASE(
    "QueryCondition: Test combinations sparse",
    "[QueryCondition][combinations][sparse]") {
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const Datatype type = Datatype::UINT64;

  // Initialize the array schema.
  ArraySchema array_schema;
  Attribute attr(field_name, type);
  REQUIRE(array_schema.add_attribute(tdb::make_shared<Attribute>(HERE(), &attr))
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
      array_schema.set_domain(make_shared<tiledb::sm::Domain>(HERE(), &domain))
          .ok());

  // Initialize the result tile.
  ResultTile result_tile(0, 0, array_schema);
  result_tile.init_attr_tile(field_name);
  ResultTile::TileTuple* const tile_tuple = result_tile.tile_tuple(field_name);
  Tile* const tile = &std::get<0>(*tile_tuple);

  // Initialize and populate the data tile.
  REQUIRE(tile->init_unfiltered(
                  constants::format_version,
                  type,
                  cells * sizeof(uint64_t),
                  sizeof(uint64_t),
                  0)
              .ok());
  uint64_t* values = static_cast<uint64_t*>(malloc(sizeof(uint64_t) * cells));
  for (uint64_t i = 0; i < cells; ++i) {
    values[i] = i;
  }
  REQUIRE(tile->write(values, 0, cells * sizeof(uint64_t)).ok());

  SECTION("Basic AND condition.") {
    // Build a combined query for `> 3 AND <= 6`.
    uint64_t cmp_value_1 = 3;
    QueryCondition query_condition_1;
    REQUIRE(query_condition_1
                .init(
                    std::string(field_name),
                    &cmp_value_1,
                    sizeof(uint64_t),
                    QueryConditionOp::GT)
                .ok());
    // Run Check for query_condition1
    REQUIRE(query_condition_1.check(array_schema).ok());
    uint64_t cmp_value_2 = 6;
    QueryCondition query_condition_2;
    REQUIRE(query_condition_2
                .init(
                    std::string(field_name),
                    &cmp_value_2,
                    sizeof(uint64_t),
                    QueryConditionOp::LE)
                .ok());
    // Run Check for query_condition2
    REQUIRE(query_condition_2.check(array_schema).ok());
    QueryCondition query_condition_3;
    REQUIRE(query_condition_1
                .combine(
                    query_condition_2,
                    QueryConditionCombinationOp::AND,
                    &query_condition_3)
                .ok());

    // Apply the query condition.
    uint64_t cell_count = 0;
    std::vector<uint8_t> result_bitmap(cells, 1);
    REQUIRE(query_condition_3
                .apply_sparse<uint8_t>(
                    array_schema, result_tile, result_bitmap, &cell_count)
                .ok());

    // Check that the cell slab now contains cell indexes 4, 5, and 6.
    REQUIRE(cell_count == 3);
    for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
      REQUIRE(
          result_bitmap[cell_idx] == (cell_idx >= 4 && cell_idx <= 6 ? 1 : 0));
    }
  }

  SECTION("Basic OR condition.") {
    // Build a combined query for `> 6 OR <= 3`.
    uint64_t cmp_value_1 = 6;
    QueryCondition query_condition_1;
    REQUIRE(query_condition_1
                .init(
                    std::string(field_name),
                    &cmp_value_1,
                    sizeof(uint64_t),
                    QueryConditionOp::GT)
                .ok());
    // Run Check for query_condition1
    REQUIRE(query_condition_1.check(array_schema).ok());
    uint64_t cmp_value_2 = 3;
    QueryCondition query_condition_2;
    REQUIRE(query_condition_2
                .init(
                    std::string(field_name),
                    &cmp_value_2,
                    sizeof(uint64_t),
                    QueryConditionOp::LE)
                .ok());
    // Run Check for query_condition2
    REQUIRE(query_condition_2.check(array_schema).ok());
    QueryCondition query_condition_3;
    REQUIRE(query_condition_1
                .combine(
                    query_condition_2,
                    QueryConditionCombinationOp::OR,
                    &query_condition_3)
                .ok());

    // Apply the query condition.
    uint64_t cell_count = 0;
    std::vector<uint8_t> result_bitmap(cells, 1);
    REQUIRE(query_condition_3
                .apply_sparse<uint8_t>(
                    array_schema, result_tile, result_bitmap, &cell_count)
                .ok());

    // Check that the cell slab now contains cell indexes 0, 1, 2, 3, and 7,
    // 8, 9.
    REQUIRE(cell_count == 7);
    for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
      REQUIRE(
          result_bitmap[cell_idx] == (cell_idx >= 7 || cell_idx <= 3 ? 1 : 0));
    }
  }

  SECTION("OR of 2 AND ASTs.") {
    // Build a combined query for `(>= 3 AND <= 6) OR (> 5 AND < 9)`.
    uint64_t cmp_value_1 = 3;
    QueryCondition query_condition_1;
    REQUIRE(query_condition_1
                .init(
                    std::string(field_name),
                    &cmp_value_1,
                    sizeof(uint64_t),
                    QueryConditionOp::GE)
                .ok());
    // Run Check for query_condition1
    REQUIRE(query_condition_1.check(array_schema).ok());
    uint64_t cmp_value_2 = 6;
    QueryCondition query_condition_2;
    REQUIRE(query_condition_2
                .init(
                    std::string(field_name),
                    &cmp_value_2,
                    sizeof(uint64_t),
                    QueryConditionOp::LE)
                .ok());
    // Run Check for query_condition2
    REQUIRE(query_condition_2.check(array_schema).ok());

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
    // Run Check for query_condition3
    REQUIRE(query_condition_3.check(array_schema).ok());
    uint64_t cmp_value_4 = 9;
    QueryCondition query_condition_4;
    REQUIRE(query_condition_4
                .init(
                    std::string(field_name),
                    &cmp_value_4,
                    sizeof(uint64_t),
                    QueryConditionOp::LT)
                .ok());
    // Run Check for query_condition2
    REQUIRE(query_condition_4.check(array_schema).ok());
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

    // Apply the query condition.
    uint64_t cell_count = 0;
    std::vector<uint8_t> result_bitmap(cells, 1);
    REQUIRE(combined_or
                .apply_sparse<uint8_t>(
                    array_schema, result_tile, result_bitmap, &cell_count)
                .ok());

    // Check that the cell slab now contains cell indexes 3, 4, 5, 6, 7, 8.
    REQUIRE(cell_count == 6);
    for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
      REQUIRE(
          result_bitmap[cell_idx] == (cell_idx >= 3 && cell_idx <= 8 ? 1 : 0));
    }
  }

  SECTION("AND of 2 OR ASTs.") {
    // Build a combined query for `(< 3 OR >= 8) AND (<= 4 OR = 9)`.
    uint64_t cmp_value_1 = 3;
    QueryCondition query_condition_1;
    REQUIRE(query_condition_1
                .init(
                    std::string(field_name),
                    &cmp_value_1,
                    sizeof(uint64_t),
                    QueryConditionOp::LT)
                .ok());
    // Run Check for query_condition1
    REQUIRE(query_condition_1.check(array_schema).ok());
    uint64_t cmp_value_2 = 8;
    QueryCondition query_condition_2;
    REQUIRE(query_condition_2
                .init(
                    std::string(field_name),
                    &cmp_value_2,
                    sizeof(uint64_t),
                    QueryConditionOp::GE)
                .ok());
    // Run Check for query_condition2
    REQUIRE(query_condition_2.check(array_schema).ok());

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
    // Run Check for query_condition3
    REQUIRE(query_condition_3.check(array_schema).ok());
    uint64_t cmp_value_4 = 9;
    QueryCondition query_condition_4;
    REQUIRE(query_condition_4
                .init(
                    std::string(field_name),
                    &cmp_value_4,
                    sizeof(uint64_t),
                    QueryConditionOp::EQ)
                .ok());
    // Run Check for query_condition2
    REQUIRE(query_condition_4.check(array_schema).ok());
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

    // Apply the query condition.
    uint64_t cell_count = 0;
    std::vector<uint8_t> result_bitmap(cells, 1);
    REQUIRE(combined_and
                .apply_sparse<uint8_t>(
                    array_schema, result_tile, result_bitmap, &cell_count)
                .ok());

    // Check that the cell slab now contains cell indexes 0, 1, 2, 9.
    REQUIRE(cell_count == 4);
    for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
      REQUIRE(
          result_bitmap[cell_idx] == (cell_idx <= 2 || cell_idx == 9 ? 1 : 0));
    }
  }

  SECTION("Complex tree with depth > 2.") {
    // Build a combined query for (((x = 1 || x = 2) && (x = 3 || x = 4)) ||
    // (((x
    // != 8 && x != 9 && (x = 6 || x = 7)) || x = 5) && x != 6))
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

    // Apply the query condition.
    uint64_t cell_count = 0;
    std::vector<uint8_t> result_bitmap(cells, 1);
    REQUIRE(qc.apply_sparse<uint8_t>(
                  array_schema, result_tile, result_bitmap, &cell_count)
                .ok());

    // Check that the cell slab now contains cell indexes 5 and 7.
    REQUIRE(cell_count == 2);
    for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
      REQUIRE(
          result_bitmap[cell_idx] == (cell_idx == 5 || cell_idx == 7 ? 1 : 0));
    }
  }

  SECTION("Adding simple clauses to AND tree.") {
    // foo != 1 && foo != 3 && foo != 5 && foo != 7 && foo != 9
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

    // Apply the query condition.
    uint64_t cell_count = 0;
    std::vector<uint8_t> result_bitmap(cells, 1);
    REQUIRE(combined_and4
                .apply_sparse<uint8_t>(
                    array_schema, result_tile, result_bitmap, &cell_count)
                .ok());

    // Check that the cell slab now contains cell indexes 0, 2, 4, 6, 8.
    REQUIRE(cell_count == 5);
    for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
      REQUIRE(result_bitmap[cell_idx] == (cell_idx % 2 == 0 ? 1 : 0));
    }
  }

  SECTION("Adding simple clauses to OR tree.") {
    // foo = 0 || foo = 2 || foo = 4 || foo = 6 || foo = 8
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

    // Apply the query condition.
    uint64_t cell_count = 0;
    std::vector<uint8_t> result_bitmap(cells, 1);
    REQUIRE(combined_or4
                .apply_sparse<uint8_t>(
                    array_schema, result_tile, result_bitmap, &cell_count)
                .ok());

    // Check that the cell slab now contains cell indexes 0, 2, 4, 6, 8.
    REQUIRE(cell_count == 5);
    for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
      REQUIRE(result_bitmap[cell_idx] == (cell_idx % 2 == 0 ? 1 : 0));
    }
  }

  free(values);
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
  ArraySchema array_schema;
  Attribute attr(field_name, type);
  REQUIRE(attr.set_nullable(nullable).ok());
  REQUIRE(attr.set_cell_val_num(var_size ? constants::var_num : 2).ok());

  if (!nullable) {
    REQUIRE(attr.set_fill_value(fill_value, 2 * sizeof(char)).ok());
  }

  REQUIRE(
      array_schema.add_attribute(make_shared<Attribute>(HERE(), &attr)).ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(domain.add_dimension(make_shared<Dimension>(HERE(), &dim)).ok());
  REQUIRE(array_schema.set_domain(make_shared<Domain>(HERE(), &domain)).ok());

  // Initialize the result tile.
  ResultTile result_tile(0, 0, array_schema);
  result_tile.init_attr_tile(field_name);

  ResultTile::TileTuple* const tile_tuple = result_tile.tile_tuple(field_name);

  var_size = array_schema.attribute(field_name)->var_size();
  nullable = array_schema.attribute(field_name)->nullable();
  Tile* const tile =
      var_size ? &std::get<1>(*tile_tuple) : &std::get<0>(*tile_tuple);

  REQUIRE(tile->init_unfiltered(
                  constants::format_version,
                  type,
                  2 * (cells - 2) * sizeof(char),
                  2 * sizeof(char),
                  0)
              .ok());

  char* values = static_cast<char*>(malloc(
      sizeof(char) * 2 *
      (cells - 2)));  // static_cast<char*>(malloc(sizeof(char) * 2 * cells));

  // Empty strings are at idx 8 and 9
  for (uint64_t i = 0; i < (cells - 2); ++i) {
    values[i * 2] = 'a';
    values[(i * 2) + 1] = 'a' + static_cast<char>(i);
  }

  REQUIRE(tile->write(values, 0, 2 * (cells - 2) * sizeof(char)).ok());

  if (var_size) {
    Tile* const tile_offsets = &std::get<0>(*tile_tuple);
    REQUIRE(tile_offsets
                ->init_unfiltered(
                    constants::format_version,
                    constants::cell_var_offset_type,
                    10 * constants::cell_var_offset_size,
                    constants::cell_var_offset_size,
                    0)
                .ok());

    uint64_t* offsets =
        static_cast<uint64_t*>(malloc(sizeof(uint64_t) * cells));
    uint64_t offset = 0;
    for (uint64_t i = 0; i < cells - 2; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    offsets[cells - 2] = offset;
    offsets[cells - 1] = offset;
    REQUIRE(tile_offsets->write(offsets, 0, cells * sizeof(uint64_t)).ok());

    free(offsets);
  }

  if (nullable) {
    Tile* const tile_validity = &std::get<2>(*tile_tuple);
    REQUIRE(tile_validity
                ->init_unfiltered(
                    constants::format_version,
                    constants::cell_validity_type,
                    10 * constants::cell_validity_size,
                    constants::cell_validity_size,
                    0)
                .ok());

    uint8_t* validity = static_cast<uint8_t*>(malloc(sizeof(uint8_t) * cells));
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE(tile_validity->write(validity, 0, cells * sizeof(uint8_t)).ok());

    free(validity);
  }

  // Empty string or null string as condition value
  const char* cmp_value = null_cmp ? nullptr : "";

  QueryCondition query_condition;
  REQUIRE(query_condition.init(std::string(field_name), cmp_value, 0, op).ok());

  // Run Check for query_condition
  REQUIRE(query_condition.check(array_schema).ok());

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
  uint64_t cell_count = 0;
  std::vector<uint8_t> result_bitmap(cells, 1);
  REQUIRE(query_condition
              .apply_sparse<uint8_t>(
                  array_schema, result_tile, result_bitmap, &cell_count)
              .ok());

  // Verify the result bitmap contain the expected cells.
  REQUIRE(cell_count == expected_cell_idx_vec.size());
  auto expected_iter = expected_cell_idx_vec.begin();
  for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
    if (result_bitmap[cell_idx]) {
      REQUIRE(*expected_iter == cell_idx);
      ++expected_iter;
    }
  }

  free(values);
}