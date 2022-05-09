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

#include "test/src/helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/query/ast/query_ast.h"

#include "catch.hpp"

using namespace tiledb::sm;
using namespace tiledb::test;

TEST_CASE("ASTNode Constructors", "[QueryCondition][ast][constructor]") {
  // ASTNodeVal constructor
  std::string field_name = "x";
  int val = 5;
  auto node_val = tdb_unique_ptr<ASTNodeVal>(
      tdb_new(ASTNodeVal, field_name, &val, sizeof(val), QueryConditionOp::LT));
  REQUIRE(ast_node_to_str(node_val) == "x LT 05 00 00 00");

  // ASTNodeVal constructor
  std::string field_name1 = "y";
  int val1 = 3;
  auto node_val1 = tdb_unique_ptr<ASTNodeVal>(tdb_new(
      ASTNodeVal, field_name1, &val1, sizeof(val1), QueryConditionOp::GT));
  REQUIRE(ast_node_to_str(node_val1) == "y GT 03 00 00 00");

  // ASTNodeExpr constructor
  auto combined_node =
      node_val->combine(node_val1, QueryConditionCombinationOp::AND);
  REQUIRE(
      ast_node_to_str(combined_node) ==
      "(x LT 05 00 00 00 AND y GT 03 00 00 00)");

  // ASTNodeVal constructor
  std::string field_name2 = "a";
  int val2 = 23;
  auto node_val2 = tdb_unique_ptr<ASTNodeVal>(tdb_new(
      ASTNodeVal, field_name2, &val2, sizeof(val2), QueryConditionOp::EQ));
  REQUIRE(ast_node_to_str(node_val2) == "a EQ 17 00 00 00");

  // ASTNodeVal constructor
  std::string field_name3 = "b";
  int val3 = 2;
  auto node_val3 = tdb_unique_ptr<ASTNodeVal>(tdb_new(
      ASTNodeVal, field_name3, &val3, sizeof(val3), QueryConditionOp::NE));
  REQUIRE(ast_node_to_str(node_val3) == "b NE 02 00 00 00");

  std::string field_name4 = "c";
  int val4 = 8;
  auto node_val4 = tdb_unique_ptr<ASTNodeVal>(tdb_new(
      ASTNodeVal, field_name4, &val4, sizeof(val4), QueryConditionOp::LE));
  REQUIRE(ast_node_to_str(node_val4) == "c LE 08 00 00 00");

  auto combined_node_inter1 =
      node_val2->combine(node_val3, QueryConditionCombinationOp::OR);
  auto combined_node1 =
      node_inter1->combine(node_val4, QueryConditionCombinationOp::OR);
  REQUIRE(
      ast_node_to_str(combined_node1) ==
      "(a EQ 17 00 00 00 OR b NE 02 00 00 00 OR c LE 08 00 00 00)");

  auto combined_node2 =
      combined_node->combine(combined_node1, QueryConditionCombinationOp::OR);
  REQUIRE(
      ast_node_to_str(combined_node2) ==
      "((x LT 05 00 00 00 AND y GT 03 00 00 00) OR (a EQ 17 00 00 00 OR b NE "
      "02 00 00 00 OR c LE 08 00 00 00))");

  auto clone_combined_node2 = combined_node2->clone();
  REQUIRE(
      ast_node_to_str(clone_combined_node2) ==
      "((x LT 05 00 00 00 AND y GT 03 00 00 00) OR (a EQ 17 00 00 00 OR b NE "
      "02 00 00 00 OR c LE 08 00 00 00))");
}

TEST_CASE(
    "AST Constructors with depth > 2", "[QueryCondition][ast][constructor]") {
  std::vector<int> vals = {1, 2, 3, 4, 5, 6, 7, 8, 9};
  std::vector<tdb_unique_ptr<ASTNodeVal>> ast_value_vector;
  for (size_t i = 0; i < 7; ++i) {
    auto node_val = tdb_unique_ptr<ASTNodeVal>(tdb_new(
        ASTNodeVal, "x", &vals[i], sizeof(vals[i]), QueryConditionOp::EQ));
    ast_value_vector.push_back(std::move(node_val));
  }

  for (size_t i = 7; i < vals.size(); ++i) {
    auto node_val = tdb_unique_ptr<ASTNodeVal>(tdb_new(
        ASTNodeVal, "x", &vals[i], sizeof(vals[i]), QueryConditionOp::NE));
    ast_value_vector.push_back(std::move(node_val));
  }

  int x = 6;
  auto x_neq_six = tdb_unique_ptr<ASTNodeVal>(
      tdb_new(ASTNodeVal, "x", &x, sizeof(x), QueryConditionOp::NE));

  auto one_or_two = ast_value_vector[0]->clone()->combine(
      ast_value_vector[1]->clone(), QueryConditionCombinationOp::OR);
  auto three_or_four = ast_value_vector[2]->clone()->combine(
      ast_value_vector[3]->clone(), QueryConditionCombinationOp::OR);
  auto six_or_seven = ast_value_vector[5]->clone()->combine(
      ast_value_vector[6]->clone(), QueryConditionCombinationOp::OR);
  auto eight_and_nine = ast_value_vector[7]->clone()->combine(
      ast_value_vector[8]->clone(), QueryConditionCombinationOp::AND);

  auto subtree_a =
      one_or_two->combine(three_or_four, QueryConditionCombinationOp::AND);
  auto subtree_d =
      eight_and_nine->combine(six_or_seven, QueryConditionCombinationOp::AND);
  auto subtree_c = subtree_d->combine(
      ast_value_vector[4]->clone(), QueryConditionCombinationOp::OR);
  auto subtree_b =
      subtree_c->combine(x_neq_six, QueryConditionCombinationOp::AND);
  auto tree = subtree_a->combine(subtree_b, QueryConditionCombinationOp::OR);

  std::cout << ast_node_to_str(tree) << std::endl;
}
