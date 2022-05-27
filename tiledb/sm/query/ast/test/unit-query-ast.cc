/**
 * @file unit-query-ast.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Tests the `ASTNode`, `ASTNodeVal` and `ASTNodeExpr` classes.
 */
#include <iostream>

#include "test/src/ast_helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/query/ast/query_ast.h"

#include "catch.hpp"

using namespace tiledb::sm;
using namespace tiledb::test;

TEST_CASE("ASTNode Constructors", "[QueryCondition][ast][constructor]") {
  // ASTNodeVal constructor
  std::string field_name = "x";
  int val = 5;
  auto node_val = tdb_unique_ptr<ASTNode>(
      tdb_new(ASTNodeVal, field_name, &val, sizeof(val), QueryConditionOp::LT));
  CHECK(ast_node_to_str(node_val) == "x LT 05 00 00 00");

  // ASTNodeVal constructor
  std::string field_name1 = "y";
  int val1 = 3;
  auto node_val1 = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal, field_name1, &val1, sizeof(val1), QueryConditionOp::GT));
  CHECK(ast_node_to_str(node_val1) == "y GT 03 00 00 00");

  // ASTNodeExpr constructor
  auto combined_node =
      node_val->combine(node_val1, QueryConditionCombinationOp::AND);
  CHECK(
      ast_node_to_str(combined_node) ==
      "(x LT 05 00 00 00 AND y GT 03 00 00 00)");

  // ASTNodeVal constructor
  std::string field_name2 = "a";
  int val2 = 23;
  auto node_val2 = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal, field_name2, &val2, sizeof(val2), QueryConditionOp::EQ));
  CHECK(ast_node_to_str(node_val2) == "a EQ 17 00 00 00");

  // ASTNodeVal constructor
  std::string field_name3 = "b";
  int val3 = 2;
  auto node_val3 = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal, field_name3, &val3, sizeof(val3), QueryConditionOp::NE));
  CHECK(ast_node_to_str(node_val3) == "b NE 02 00 00 00");

  std::string field_name4 = "c";
  int val4 = 8;
  auto node_val4 = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal, field_name4, &val4, sizeof(val4), QueryConditionOp::LE));
  CHECK(ast_node_to_str(node_val4) == "c LE 08 00 00 00");

  auto combined_node_inter1 =
      node_val2->combine(node_val3, QueryConditionCombinationOp::OR);
  auto combined_node1 =
      combined_node_inter1->combine(node_val4, QueryConditionCombinationOp::OR);
  CHECK(
      ast_node_to_str(combined_node1) ==
      "(a EQ 17 00 00 00 OR b NE 02 00 00 00 OR c LE 08 00 00 00)");

  auto combined_node2 =
      combined_node->combine(combined_node1, QueryConditionCombinationOp::OR);
  CHECK(
      ast_node_to_str(combined_node2) ==
      "((x LT 05 00 00 00 AND y GT 03 00 00 00) OR a EQ 17 00 00 00 OR b NE "
      "02 00 00 00 OR c LE 08 00 00 00)");

  auto clone_combined_node2 = combined_node2->clone();
  CHECK(
      ast_node_to_str(clone_combined_node2) ==
      "((x LT 05 00 00 00 AND y GT 03 00 00 00) OR a EQ 17 00 00 00 OR b NE "
      "02 00 00 00 OR c LE 08 00 00 00)");
}

TEST_CASE(
    "ASTNode Constructors, adding simple clauses to an AND tree",
    "[QueryCondition][ast][constructor]") {
  // foo != 1 && foo != 3 && foo != 5 && foo != 7 && foo != 9
  std::string field_name1 = "foo";
  int val1 = 1;
  auto query_condition1 = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal,
      std::string(field_name1),
      &val1,
      sizeof(int),
      QueryConditionOp::NE));
  CHECK(ast_node_to_str(query_condition1) == "foo NE 01 00 00 00");

  std::string field_name2 = "foo";
  int val2 = 3;
  auto query_condition2 = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal,
      std::string(field_name2),
      &val2,
      sizeof(int),
      QueryConditionOp::NE));
  CHECK(ast_node_to_str(query_condition2) == "foo NE 03 00 00 00");

  std::string field_name3 = "foo";
  int val3 = 5;
  auto query_condition3 = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal,
      std::string(field_name3),
      &val3,
      sizeof(int),
      QueryConditionOp::NE));
  CHECK(ast_node_to_str(query_condition3) == "foo NE 05 00 00 00");

  std::string field_name4 = "foo";
  int val4 = 7;
  auto query_condition4 = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal,
      std::string(field_name4),
      &val4,
      sizeof(int),
      QueryConditionOp::NE));
  CHECK(ast_node_to_str(query_condition4) == "foo NE 07 00 00 00");

  std::string field_name5 = "foo";
  int val5 = 9;
  auto query_condition5 = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal,
      std::string(field_name5),
      &val5,
      sizeof(int),
      QueryConditionOp::NE));
  CHECK(ast_node_to_str(query_condition5) == "foo NE 09 00 00 00");

  auto combined_and1 = query_condition1->combine(
      query_condition2, QueryConditionCombinationOp::AND);
  CHECK(
      ast_node_to_str(combined_and1) ==
      "(foo NE 01 00 00 00 AND foo NE 03 00 00 00)");

  auto combined_and2 = combined_and1->combine(
      query_condition3, QueryConditionCombinationOp::AND);
  CHECK(
      ast_node_to_str(combined_and2) ==
      "(foo NE 01 00 00 00 AND foo NE 03 00 00 00 AND foo NE 05 00 00 00)");

  auto combined_and3 = combined_and2->combine(
      query_condition4, QueryConditionCombinationOp::AND);
  CHECK(
      ast_node_to_str(combined_and3) ==
      "(foo NE 01 00 00 00 AND foo NE 03 00 00 00 AND foo NE 05 00 00 00 AND "
      "foo NE 07 00 00 00)");

  auto combined_and4 = combined_and3->combine(
      query_condition5, QueryConditionCombinationOp::AND);
  CHECK(
      ast_node_to_str(combined_and4) ==
      "(foo NE 01 00 00 00 AND foo NE 03 00 00 00 AND foo NE 05 00 00 00 AND "
      "foo NE 07 00 00 00 AND foo NE 09 00 00 00)");
}

TEST_CASE(
    "ASTNode Constructors, adding simple clauses to an OR tree",
    "[QueryCondition][ast][constructor]") {
  // foo = 0 || foo = 2 || foo = 4 || foo = 6 || foo = 8
  std::string field_name1 = "foo";
  int val1 = 0;
  auto query_condition1 = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal,
      std::string(field_name1),
      &val1,
      sizeof(int),
      QueryConditionOp::EQ));
  CHECK(ast_node_to_str(query_condition1) == "foo EQ 00 00 00 00");

  std::string field_name2 = "foo";
  int val2 = 2;
  auto query_condition2 = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal,
      std::string(field_name2),
      &val2,
      sizeof(int),
      QueryConditionOp::EQ));
  CHECK(ast_node_to_str(query_condition2) == "foo EQ 02 00 00 00");

  std::string field_name3 = "foo";
  int val3 = 4;
  auto query_condition3 = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal,
      std::string(field_name3),
      &val3,
      sizeof(int),
      QueryConditionOp::EQ));
  CHECK(ast_node_to_str(query_condition3) == "foo EQ 04 00 00 00");

  std::string field_name4 = "foo";
  int val4 = 6;
  auto query_condition4 = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal,
      std::string(field_name4),
      &val4,
      sizeof(int),
      QueryConditionOp::EQ));
  CHECK(ast_node_to_str(query_condition4) == "foo EQ 06 00 00 00");

  std::string field_name5 = "foo";
  int val5 = 8;
  auto query_condition5 = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal,
      std::string(field_name5),
      &val5,
      sizeof(int),
      QueryConditionOp::EQ));
  CHECK(ast_node_to_str(query_condition5) == "foo EQ 08 00 00 00");

  auto combined_or1 = query_condition1->combine(
      query_condition2, QueryConditionCombinationOp::OR);
  CHECK(
      ast_node_to_str(combined_or1) ==
      "(foo EQ 00 00 00 00 OR foo EQ 02 00 00 00)");

  auto combined_or2 =
      combined_or1->combine(query_condition3, QueryConditionCombinationOp::OR);
  CHECK(
      ast_node_to_str(combined_or2) ==
      "(foo EQ 00 00 00 00 OR foo EQ 02 00 00 00 OR foo EQ 04 00 00 00)");
  auto combined_or3 =
      combined_or2->combine(query_condition4, QueryConditionCombinationOp::OR);
  CHECK(
      ast_node_to_str(combined_or3) ==
      "(foo EQ 00 00 00 00 OR foo EQ 02 00 00 00 OR foo EQ 04 00 00 00 OR foo "
      "EQ 06 00 00 00)");
  auto combined_or4 =
      combined_or3->combine(query_condition5, QueryConditionCombinationOp::OR);
  CHECK(
      ast_node_to_str(combined_or4) ==
      "(foo EQ 00 00 00 00 OR foo EQ 02 00 00 00 OR foo EQ 04 00 00 00 OR foo "
      "EQ 06 00 00 00 OR foo EQ 08 00 00 00)");
}

TEST_CASE(
    "ASTNode Constructors, complex tree with depth > 2",
    "[QueryCondition][ast][constructor]") {
  std::vector<int> vals = {1, 2, 3, 4, 5, 6, 7, 8, 9};
  std::vector<tdb_unique_ptr<ASTNode>> ast_value_vector;
  for (size_t i = 0; i < 7; ++i) {
    auto node_val = tdb_unique_ptr<ASTNode>(tdb_new(
        ASTNodeVal, "x", &vals[i], sizeof(vals[i]), QueryConditionOp::EQ));
    CHECK(
        ast_node_to_str(node_val) ==
        "x EQ 0" + std::to_string(vals[i]) + " 00 00 00");
    ast_value_vector.push_back(std::move(node_val));
  }

  for (size_t i = 7; i < vals.size(); ++i) {
    auto node_val = tdb_unique_ptr<ASTNode>(tdb_new(
        ASTNodeVal, "x", &vals[i], sizeof(vals[i]), QueryConditionOp::NE));
    CHECK(
        ast_node_to_str(node_val) ==
        "x NE 0" + std::to_string(vals[i]) + " 00 00 00");
    ast_value_vector.push_back(std::move(node_val));
  }

  int x = 6;
  auto x_neq_six = tdb_unique_ptr<ASTNode>(
      tdb_new(ASTNodeVal, "x", &x, sizeof(x), QueryConditionOp::NE));
  CHECK(ast_node_to_str(x_neq_six) == "x NE 06 00 00 00");

  auto one_or_two = ast_value_vector[0]->combine(
      ast_value_vector[1]->clone(), QueryConditionCombinationOp::OR);
  CHECK(
      ast_node_to_str(one_or_two) == "(x EQ 01 00 00 00 OR x EQ 02 00 00 00)");

  auto three_or_four = ast_value_vector[2]->combine(
      ast_value_vector[3]->clone(), QueryConditionCombinationOp::OR);
  CHECK(
      ast_node_to_str(three_or_four) ==
      "(x EQ 03 00 00 00 OR x EQ 04 00 00 00)");

  auto six_or_seven = ast_value_vector[5]->combine(
      ast_value_vector[6]->clone(), QueryConditionCombinationOp::OR);

  CHECK(
      ast_node_to_str(six_or_seven) ==
      "(x EQ 06 00 00 00 OR x EQ 07 00 00 00)");

  auto eight_and_nine = ast_value_vector[7]->combine(
      ast_value_vector[8]->clone(), QueryConditionCombinationOp::AND);
  CHECK(
      ast_node_to_str(eight_and_nine) ==
      "(x NE 08 00 00 00 AND x NE 09 00 00 00)");

  auto subtree_a =
      one_or_two->combine(three_or_four, QueryConditionCombinationOp::AND);
  CHECK(
      ast_node_to_str(subtree_a) ==
      "((x EQ 01 00 00 00 OR x EQ 02 00 00 00) AND (x EQ 03 00 00 00 OR x EQ "
      "04 00 00 00))");

  auto subtree_d =
      eight_and_nine->combine(six_or_seven, QueryConditionCombinationOp::AND);
  CHECK(
      ast_node_to_str(subtree_d) ==
      "(x NE 08 00 00 00 AND x NE 09 00 00 00 AND (x EQ 06 00 00 00 OR x EQ 07 "
      "00 00 00))");

  auto subtree_c = subtree_d->combine(
      ast_value_vector[4]->clone(), QueryConditionCombinationOp::OR);

  CHECK(
      ast_node_to_str(subtree_c) ==
      "((x NE 08 00 00 00 AND x NE 09 00 00 00 AND (x EQ 06 00 00 00 OR x EQ "
      "07 00 00 00)) OR x EQ 05 00 00 00)");

  auto subtree_b =
      subtree_c->combine(x_neq_six, QueryConditionCombinationOp::AND);
  CHECK(
      ast_node_to_str(subtree_b) ==
      "(((x NE 08 00 00 00 AND x NE 09 00 00 00 AND (x EQ 06 00 00 00 OR x EQ "
      "07 00 00 00)) OR x EQ 05 00 00 00) AND x NE 06 00 00 00)");

  auto tree = subtree_a->combine(subtree_b, QueryConditionCombinationOp::OR);

  CHECK(
      ast_node_to_str(tree) ==
      "(((x EQ 01 00 00 00 OR x EQ 02 00 00 00) AND (x EQ 03 00 00 00 OR x EQ "
      "04 00 00 00)) OR (((x NE 08 00 00 00 AND x NE 09 00 00 00 AND (x EQ 06 "
      "00 00 00 OR x EQ 07 00 00 00)) OR x EQ 05 00 00 00) AND x NE 06 00 00 "
      "00))");
}
