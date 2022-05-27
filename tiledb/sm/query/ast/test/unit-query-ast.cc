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

#include <string>
#include <vector>

#include "test/src/ast_helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/query/ast/query_ast.h"

#include "catch.hpp"

using namespace tiledb::sm;
using namespace tiledb::test;

template <class T>
tdb_unique_ptr<ASTNode> test_value_node(
    std::string field_name,
    T* val,
    QueryConditionOp op,
    const std::string& expected_result) {
  // Test validity of construction of value node.
  auto node_val = tdb_unique_ptr<ASTNode>(
      tdb_new(ASTNodeVal, field_name, val, sizeof(T), op));
  CHECK(ast_node_to_str(node_val) == expected_result);

  // Test ASTNode::clone on the constructed node.
  auto node_val_clone = node_val->clone();
  CHECK(ast_node_to_str(node_val_clone) == expected_result);

  return node_val;
}

tdb_unique_ptr<ASTNode> test_expression_node(
    const tdb_unique_ptr<ASTNode>& lhs,
    const tdb_unique_ptr<ASTNode>& rhs,
    QueryConditionCombinationOp op,
    const std::string& expected_result) {
  // Test validity of construction of expression node.
  auto combined_node = lhs->combine(rhs, op);
  CHECK(ast_node_to_str(combined_node) == expected_result);

  // Test ASTNode::clone on the constructed node.
  auto combined_node_clone = combined_node->clone();
  CHECK(ast_node_to_str(combined_node_clone) == expected_result);

  return combined_node;
}

TEST_CASE("ASTNode Constructors, basic", "[QueryCondition][ast][constructor") {
  int val = 48;
  auto node_val =
      test_value_node("c", &val, QueryConditionOp::LE, "c LE 30 00 00 00");
}

TEST_CASE(
    "ASTNode Constructors, basic AND combine",
    "[QueryCondition][ast][constructor]") {
  // Testing (x < 5).
  int val = 5;
  auto node_val =
      test_value_node("x", &val, QueryConditionOp::LT, "x LT 05 00 00 00");

  // Testing (y > 3).
  int val1 = 3;
  auto node_val1 =
      test_value_node("y", &val1, QueryConditionOp::GT, "y GT 03 00 00 00");

  // Testing (x < 5 AND y > 3).
  auto combined_node = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::AND,
      "(x LT 05 00 00 00 AND y GT 03 00 00 00)");
}

TEST_CASE(
    "ASTNode Constructors, basic OR combine",
    "[QueryCondition][ast][constructor]") {
  // Testing (x < 5).
  int val = 5;
  auto node_val =
      test_value_node("x", &val, QueryConditionOp::LT, "x LT 05 00 00 00");

  // Testing (y > 3).
  int val1 = 3;
  auto node_val1 =
      test_value_node("y", &val1, QueryConditionOp::GT, "y GT 03 00 00 00");

  // Testing (x < 5 OR y > 3).
  auto combined_node = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::OR,
      "(x LT 05 00 00 00 OR y GT 03 00 00 00)");
}

TEST_CASE(
    "ASTNode Constructors, tree structure, AND of 2 OR ASTs",
    "[QueryCondition][ast][constructor]") {
  std::vector<int> vals = {1, 2, 3, 4};

  // Testing a <= 1, b < 2, c >= 3, and d > 4.
  auto node_val =
      test_value_node("a", &vals[0], QueryConditionOp::LE, "a LE 01 00 00 00");
  auto node_val1 =
      test_value_node("b", &vals[1], QueryConditionOp::LT, "b LT 02 00 00 00");
  auto node_val2 =
      test_value_node("c", &vals[2], QueryConditionOp::GE, "c GE 03 00 00 00");
  auto node_val3 =
      test_value_node("d", &vals[3], QueryConditionOp::GT, "d GT 04 00 00 00");

  // Testing (a <= 1 OR b < 2).
  auto node_expr = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::OR,
      "(a LE 01 00 00 00 OR b LT 02 00 00 00)");

  // Testing (c >= 3 OR d > 4).
  auto node_expr1 = test_expression_node(
      node_val2,
      node_val3,
      QueryConditionCombinationOp::OR,
      "(c GE 03 00 00 00 OR d GT 04 00 00 00)");

  // Testing ((a <= 1 OR b < 2) AND (c >= 3 OR d > 4)).
  auto node_combine = test_expression_node(
      node_expr,
      node_expr1,
      QueryConditionCombinationOp::AND,
      "((a LE 01 00 00 00 OR b LT 02 00 00 00) AND (c GE 03 00 00 00 OR d GT "
      "04 00 00 00))");
}

TEST_CASE(
    "ASTNode Constructors, tree structure, OR of 2 AND ASTs",
    "[QueryCondition][ast][constructor]") {
  std::vector<int> vals = {1, 2, 3, 4};

  // Testing a <= 1, b < 2, c >= 3, and d > 4.
  auto node_val =
      test_value_node("a", &vals[0], QueryConditionOp::LE, "a LE 01 00 00 00");
  auto node_val1 =
      test_value_node("b", &vals[1], QueryConditionOp::LT, "b LT 02 00 00 00");
  auto node_val2 =
      test_value_node("c", &vals[2], QueryConditionOp::GE, "c GE 03 00 00 00");
  auto node_val3 =
      test_value_node("d", &vals[3], QueryConditionOp::GT, "d GT 04 00 00 00");

  // Testing (a <= 1 AND b < 2).
  auto node_expr = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::AND,
      "(a LE 01 00 00 00 AND b LT 02 00 00 00)");

  // Testing (c >= 3 AND d > 4).
  auto node_expr1 = test_expression_node(
      node_val2,
      node_val3,
      QueryConditionCombinationOp::AND,
      "(c GE 03 00 00 00 AND d GT 04 00 00 00)");

  // Testing ((a <= 1 AND b < 2) OR (c >= 3 AND d > 4)).
  auto node_combine = test_expression_node(
      node_expr,
      node_expr1,
      QueryConditionCombinationOp::OR,
      "((a LE 01 00 00 00 AND b LT 02 00 00 00) OR (c GE 03 00 00 00 AND d GT "
      "04 00 00 00))");
}

TEST_CASE(
    "ASTNode Constructors, tree structure, AND of 2 AND ASTs",
    "[QueryCondition][ast][constructor]") {
  std::vector<int> vals = {1, 2, 3, 4};

  // Testing a <= 1, b < 2, c >= 3, and d > 4.
  auto node_val =
      test_value_node("a", &vals[0], QueryConditionOp::LE, "a LE 01 00 00 00");
  auto node_val1 =
      test_value_node("b", &vals[1], QueryConditionOp::LT, "b LT 02 00 00 00");
  auto node_val2 =
      test_value_node("c", &vals[2], QueryConditionOp::GE, "c GE 03 00 00 00");
  auto node_val3 =
      test_value_node("d", &vals[3], QueryConditionOp::GT, "d GT 04 00 00 00");

  // Testing (a <= 1 AND b < 2).
  auto node_expr = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::AND,
      "(a LE 01 00 00 00 AND b LT 02 00 00 00)");

  // Testing (c >= 3 AND d > 4).
  auto node_expr1 = test_expression_node(
      node_val2,
      node_val3,
      QueryConditionCombinationOp::AND,
      "(c GE 03 00 00 00 AND d GT 04 00 00 00)");

  // Testing (a <= 1 AND b < 2 AND c >= 3 AND d > 4).
  auto node_combine = test_expression_node(
      node_expr,
      node_expr1,
      QueryConditionCombinationOp::AND,
      "(a LE 01 00 00 00 AND b LT 02 00 00 00 AND c GE 03 00 00 00 AND d GT 04 "
      "00 00 00)");
}

TEST_CASE(
    "ASTNode Constructors, tree structure, OR of 2 OR ASTs",
    "[QueryCondition][ast][constructor]") {
  std::vector<int> vals = {1, 2, 3, 4};

  // Testing a <= 1, b < 2, c >= 3, and d > 4.
  auto node_val =
      test_value_node("a", &vals[0], QueryConditionOp::LE, "a LE 01 00 00 00");
  auto node_val1 =
      test_value_node("b", &vals[1], QueryConditionOp::LT, "b LT 02 00 00 00");
  auto node_val2 =
      test_value_node("c", &vals[2], QueryConditionOp::GE, "c GE 03 00 00 00");
  auto node_val3 =
      test_value_node("d", &vals[3], QueryConditionOp::GT, "d GT 04 00 00 00");

  // Testing (a <= 1 OR b < 2).
  auto node_expr = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::OR,
      "(a LE 01 00 00 00 OR b LT 02 00 00 00)");

  // Testing (c >= 3 OR d > 4).
  auto node_expr1 = test_expression_node(
      node_val2,
      node_val3,
      QueryConditionCombinationOp::OR,
      "(c GE 03 00 00 00 OR d GT 04 00 00 00)");

  // Testing (a <= 1 OR b < 2 OR c >= 3 OR d > 4).
  auto node_combine = test_expression_node(
      node_expr,
      node_expr1,
      QueryConditionCombinationOp::OR,
      "(a LE 01 00 00 00 OR b LT 02 00 00 00 OR c GE 03 00 00 00 OR d GT 04 00 "
      "00 00)");
}

TEST_CASE(
    "ASTNode Constructors, complex tree",
    "[QueryCondition][ast][constructor]") {
  // Testing (x < 5).
  int val = 5;
  auto node_val =
      test_value_node("x", &val, QueryConditionOp::LT, "x LT 05 00 00 00");

  // Testing (y > 3).
  int val1 = 3;
  auto node_val1 =
      test_value_node("y", &val1, QueryConditionOp::GT, "y GT 03 00 00 00");

  // Testing (x < 5 AND y > 3).
  auto combined_node = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::AND,
      "(x LT 05 00 00 00 AND y GT 03 00 00 00)");

  // Testing (a = 23).
  int val2 = 23;
  auto node_val2 =
      test_value_node("a", &val2, QueryConditionOp::EQ, "a EQ 17 00 00 00");

  // Testing (b != 2);
  int val3 = 2;
  auto node_val3 =
      test_value_node("b", &val3, QueryConditionOp::NE, "b NE 02 00 00 00");

  // Testing (c <= 8).
  int val4 = 8;
  auto node_val4 =
      test_value_node("c", &val4, QueryConditionOp::LE, "c LE 08 00 00 00");

  // Testing (a = 23 OR b != 2 OR c <= 8).
  auto combined_node_inter1 = test_expression_node(
      node_val2,
      node_val3,
      QueryConditionCombinationOp::OR,
      "(a EQ 17 00 00 00 OR b NE 02 00 00 00)");
  auto combined_node1 = test_expression_node(
      combined_node_inter1,
      node_val4,
      QueryConditionCombinationOp::OR,
      "(a EQ 17 00 00 00 OR b NE 02 00 00 00 OR c LE 08 00 00 00)");

  // Testing ((x < 5 AND y > 3) OR a = 23 OR b != 2 OR c <= 8).
  test_expression_node(
      combined_node,
      combined_node1,
      QueryConditionCombinationOp::OR,
      "((x LT 05 00 00 00 AND y GT 03 00 00 00) OR a EQ 17 00 00 00 OR b NE 02 "
      "00 00 00 OR c LE 08 00 00 00)");
}

TEST_CASE(
    "ASTNode Constructors, adding simple clauses to an AND tree",
    "[QueryCondition][ast][constructor]") {
  std::vector<int> vals = {1, 3, 5, 7, 9};
  std::vector<std::string> expected_strs = {"foo NE 01 00 00 00",
                                            "foo NE 03 00 00 00",
                                            "foo NE 05 00 00 00",
                                            "foo NE 07 00 00 00",
                                            "foo NE 09 00 00 00"};
  std::vector<tdb_unique_ptr<ASTNode>> ast_val_nodes;

  // Creating the value nodes foo != 1, foo != 3, foo != 5, foo != 7, and foo
  // != 9.
  for (size_t i = 0; i < vals.size(); ++i) {
    ast_val_nodes.push_back(test_value_node(
        "foo", &vals[i], QueryConditionOp::NE, expected_strs[i]));
  }

  // Testing (foo != 1 AND foo != 3).
  auto combined_and1 = test_expression_node(
      ast_val_nodes[0],
      ast_val_nodes[1],
      QueryConditionCombinationOp::AND,
      "(foo NE 01 00 00 00 AND foo NE 03 00 00 00)");

  // Testing (foo != 1 AND foo != 3 AND foo ! 5).
  auto combined_and2 = test_expression_node(
      combined_and1,
      ast_val_nodes[2],
      QueryConditionCombinationOp::AND,
      "(foo NE 01 00 00 00 AND foo NE 03 00 00 00 AND foo NE 05 00 00 00)");

  // Testing (foo != 1 AND foo != 3 AND foo ! 5 AND foo != 7).
  auto combined_and3 = test_expression_node(
      combined_and2,
      ast_val_nodes[3],
      QueryConditionCombinationOp::AND,
      "(foo NE 01 00 00 00 AND foo NE 03 00 00 00 AND foo NE 05 00 00 00 AND "
      "foo NE 07 00 00 00)");

  // Testing (foo != 1 AND foo != 3 AND foo ! 5 AND foo != 7 AND foo != 9).
  auto combined_and4 = test_expression_node(
      combined_and3,
      ast_val_nodes[4],
      QueryConditionCombinationOp::AND,
      "(foo NE 01 00 00 00 AND foo NE 03 00 00 00 AND foo NE 05 00 00 00 AND "
      "foo NE 07 00 00 00 AND foo NE 09 00 00 00)");
}

TEST_CASE(
    "ASTNode Constructors, adding simple clauses to an OR tree",
    "[QueryCondition][ast][constructor]") {
  std::vector<int> vals = {0, 2, 4, 6, 8};
  std::vector<std::string> expected_strs = {"foo EQ 00 00 00 00",
                                            "foo EQ 02 00 00 00",
                                            "foo EQ 04 00 00 00",
                                            "foo EQ 06 00 00 00",
                                            "foo EQ 08 00 00 00"};
  std::vector<tdb_unique_ptr<ASTNode>> ast_val_nodes;

  // Creating the value nodes foo = 0, foo = 2, foo = 4, foo = 6, and foo = 8.
  for (size_t i = 0; i < vals.size(); ++i) {
    ast_val_nodes.push_back(test_value_node(
        "foo", &vals[i], QueryConditionOp::EQ, expected_strs[i]));
  }

  // Testing (foo = 0 OR foo = 2).
  auto combined_or1 = test_expression_node(
      ast_val_nodes[0],
      ast_val_nodes[1],
      QueryConditionCombinationOp::OR,
      "(foo EQ 00 00 00 00 OR foo EQ 02 00 00 00)");

  // Testing (foo = 0 OR foo = 2 OR foo = 4).
  auto combined_or2 = test_expression_node(
      combined_or1,
      ast_val_nodes[2],
      QueryConditionCombinationOp::OR,
      "(foo EQ 00 00 00 00 OR foo EQ 02 00 00 00 OR foo EQ 04 00 00 00)");

  // Testing (foo = 0 OR foo = 2 OR foo = 4 OR foo = 6).
  auto combined_or3 = test_expression_node(
      combined_or2,
      ast_val_nodes[3],
      QueryConditionCombinationOp::OR,
      "(foo EQ 00 00 00 00 OR foo EQ 02 00 00 00 OR foo EQ 04 00 00 00 OR foo "
      "EQ 06 00 00 00)");

  // Testing (foo = 0 OR foo = 2 OR foo = 4 OR foo = 6 OR foo = 8).
  auto combined_or4 = test_expression_node(
      combined_or3,
      ast_val_nodes[4],
      QueryConditionCombinationOp::OR,
      "(foo EQ 00 00 00 00 OR foo EQ 02 00 00 00 OR foo EQ 04 00 00 00 OR foo "
      "EQ 06 00 00 00 OR foo EQ 08 00 00 00)");
}

TEST_CASE(
    "ASTNode Constructors, complex tree with depth > 2",
    "[QueryCondition][ast][constructor]") {
  std::vector<int> vals = {1, 2, 3, 4, 5, 6, 7, 8, 9};
  std::vector<tdb_unique_ptr<ASTNode>> ast_value_vector;
  for (size_t i = 0; i < 7; ++i) {
    ast_value_vector.push_back(test_value_node(
        "x",
        &vals[i],
        QueryConditionOp::EQ,
        "x EQ 0" + std::to_string(vals[i]) + " 00 00 00"));
  }

  for (size_t i = 7; i < vals.size(); ++i) {
    ast_value_vector.push_back(test_value_node(
        "x",
        &vals[i],
        QueryConditionOp::NE,
        "x NE 0" + std::to_string(vals[i]) + " 00 00 00"));
  }

  auto x_neq_six =
      test_value_node("x", &vals[5], QueryConditionOp::NE, "x NE 06 00 00 00");
  auto one_or_two = test_expression_node(
      ast_value_vector[0],
      ast_value_vector[1],
      QueryConditionCombinationOp::OR,
      "(x EQ 01 00 00 00 OR x EQ 02 00 00 00)");
  auto three_or_four = test_expression_node(
      ast_value_vector[2],
      ast_value_vector[3],
      QueryConditionCombinationOp::OR,
      "(x EQ 03 00 00 00 OR x EQ 04 00 00 00)");
  auto six_or_seven = test_expression_node(
      ast_value_vector[5],
      ast_value_vector[6],
      QueryConditionCombinationOp::OR,
      "(x EQ 06 00 00 00 OR x EQ 07 00 00 00)");
  auto eight_and_nine = test_expression_node(
      ast_value_vector[7],
      ast_value_vector[8],
      QueryConditionCombinationOp::AND,
      "(x NE 08 00 00 00 AND x NE 09 00 00 00)");

  auto subtree_a = test_expression_node(
      one_or_two,
      three_or_four,
      QueryConditionCombinationOp::AND,
      "((x EQ 01 00 00 00 OR x EQ 02 00 00 00) AND (x EQ 03 00 00 00 OR x EQ "
      "04 00 00 00))");
  auto subtree_d = test_expression_node(
      eight_and_nine,
      six_or_seven,
      QueryConditionCombinationOp::AND,
      "(x NE 08 00 00 00 AND x NE 09 00 00 00 AND (x EQ 06 00 00 00 OR x EQ 07 "
      "00 00 00))");
  auto subtree_c = test_expression_node(
      subtree_d,
      ast_value_vector[4],
      QueryConditionCombinationOp::OR,
      "((x NE 08 00 00 00 AND x NE 09 00 00 00 AND (x EQ 06 00 00 00 OR x EQ "
      "07 00 00 00)) OR x EQ 05 00 00 00)");
  auto subtree_b = test_expression_node(
      subtree_c,
      x_neq_six,
      QueryConditionCombinationOp::AND,
      "(((x NE 08 00 00 00 AND x NE 09 00 00 00 AND (x EQ 06 00 00 00 OR x EQ "
      "07 00 00 00)) OR x EQ 05 00 00 00) AND x NE 06 00 00 00)");

  auto tree = test_expression_node(
      subtree_a,
      subtree_b,
      QueryConditionCombinationOp::OR,
      "(((x EQ 01 00 00 00 OR x EQ 02 00 00 00) AND (x EQ 03 00 00 00 OR x EQ "
      "04 00 00 00)) OR (((x NE 08 00 00 00 AND x NE 09 00 00 00 AND (x EQ 06 "
      "00 00 00 OR x EQ 07 00 00 00)) OR x EQ 05 00 00 00) AND x NE 06 00 00 "
      "00))");
}
