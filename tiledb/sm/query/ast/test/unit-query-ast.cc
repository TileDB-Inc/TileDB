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

#include <cstring>
#include <string>
#include <vector>

#include "test/support/src/ast_helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/query/ast/query_ast.h"

#include <test/support/tdb_catch.h>

using namespace tiledb::sm;
using namespace tiledb::test;

template <class T>
tdb_unique_ptr<ASTNode> test_value_node(
    std::string field_name,
    T* val,
    QueryConditionOp op,
    const std::string& expected_result,
    bool negate = false) {
  // Test validity of construction of value node.
  auto node_val = tdb_unique_ptr<ASTNode>(
      tdb_new(ASTNodeVal, field_name, val, sizeof(T), op));

  if (!negate) {
    CHECK(ast_node_to_str(node_val) == expected_result);
  }

  // Test ASTNode::clone on the constructed node.
  tdb_unique_ptr<ASTNode> node_val_clone;
  if (negate) {
    node_val_clone = node_val->get_negated_tree();
  } else {
    node_val_clone = node_val->clone();
  }
  CHECK(ast_node_to_str(node_val_clone) == expected_result);

  return node_val;
}

tdb_unique_ptr<ASTNode> test_string_value_node(
    std::string field_name,
    char* val,
    QueryConditionOp op,
    const std::string& expected_result,
    bool negate = false) {
  if (val == nullptr) {
    throw std::runtime_error("test_string_value_node: val cannot be null.");
  }
  // Test validity of construction of value node.
  auto node_val = tdb_unique_ptr<ASTNode>(
      tdb_new(ASTNodeVal, field_name, val, strlen(val), op));
  if (!negate) {
    CHECK(ast_node_to_str(node_val) == expected_result);
  }

  // Test ASTNode::clone on the constructed node.
  tdb_unique_ptr<ASTNode> node_val_clone;
  if (negate) {
    node_val_clone = node_val->get_negated_tree();
  } else {
    node_val_clone = node_val->clone();
  }
  CHECK(ast_node_to_str(node_val_clone) == expected_result);

  return node_val;
}

tdb_unique_ptr<ASTNode> test_expression_node(
    const tdb_unique_ptr<ASTNode>& lhs,
    const tdb_unique_ptr<ASTNode>& rhs,
    QueryConditionCombinationOp op,
    const std::string& expected_result,
    bool negate = false) {
  // Test validity of construction of expression node.
  auto combined_node = lhs->combine(*rhs, op);

  if (!negate) {
    CHECK(ast_node_to_str(combined_node) == expected_result);
  }

  // Test ASTNode::clone on the constructed node.
  tdb_unique_ptr<ASTNode> combined_node_clone;
  if (negate) {
    combined_node_clone = combined_node->get_negated_tree();
  } else {
    combined_node_clone = combined_node->clone();
  }
  CHECK(ast_node_to_str(combined_node_clone) == expected_result);

  return combined_node;
}

TEST_CASE("ASTNode Constructors, basic", "[QueryCondition][ast][constructor]") {
  int val = 0x12345678;
  auto node_val =
      test_value_node("c", &val, QueryConditionOp::LE, "c LE 78 56 34 12");
}

TEST_CASE(
    "ASTNode Constructors, string", "[QueryCondition][ast][constructor]") {
  // Test validity of construction of value node.
  char value[] = "bar";
  auto node_val = test_string_value_node(
      "foo",
      static_cast<char*>(value),
      QueryConditionOp::LE,
      "foo LE 62 61 72");
}

TEST_CASE(
    "ASTNode Constructors, basic AND combine",
    "[QueryCondition][ast][constructor]") {
  // Testing (x < 0xabcdef12).
  int val = 0xabcdef12;
  auto node_val =
      test_value_node("x", &val, QueryConditionOp::LT, "x LT 12 ef cd ab");

  // Testing (y > 0x33333333).
  int val1 = 0x33333333;
  auto node_val1 =
      test_value_node("y", &val1, QueryConditionOp::GT, "y GT 33 33 33 33");

  // Testing (x < 0xabcdef12 AND y > 0x33333333).
  auto combined_node = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::AND,
      "(x LT 12 ef cd ab AND y GT 33 33 33 33)");
}

TEST_CASE(
    "ASTNode Constructors, basic OR combine",
    "[QueryCondition][ast][constructor]") {
  // Testing (x < 0xabcdef12).
  int val = 0xabcdef12;
  auto node_val =
      test_value_node("x", &val, QueryConditionOp::LT, "x LT 12 ef cd ab");

  // Testing (y > 0x33333333).
  int val1 = 0x33333333;
  auto node_val1 =
      test_value_node("y", &val1, QueryConditionOp::GT, "y GT 33 33 33 33");

  // Testing (x < 0xabcdef12 AND y > 0x33333333).
  auto combined_node = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::OR,
      "(x LT 12 ef cd ab OR y GT 33 33 33 33)");
}

TEST_CASE(
    "ASTNode Constructors, basic AND combine, strings",
    "[QueryCondition][ast][constructor]") {
  // Testing (x < "eve").
  char e[] = "eve";
  auto node_val =
      test_string_value_node("x", e, QueryConditionOp::LT, "x LT 65 76 65");

  // Testing (x > "bob").
  char b[] = "bob";
  auto node_val1 =
      test_string_value_node("x", b, QueryConditionOp::GT, "x GT 62 6f 62");

  // Testing (x < "eve" AND x > "bob").
  auto combined_node = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::AND,
      "(x LT 65 76 65 AND x GT 62 6f 62)");
}

TEST_CASE(
    "ASTNode Constructors, basic OR combine, strings",
    "[QueryCondition][ast][constructor]") {
  // Testing (x < "eve").
  char e[] = "eve";
  auto node_val =
      test_string_value_node("x", e, QueryConditionOp::LT, "x LT 65 76 65");

  // Testing (x > "bob").
  char b[] = "bob";
  auto node_val1 =
      test_string_value_node("x", b, QueryConditionOp::GT, "x GT 62 6f 62");

  // Testing (x < "eve" OR x > "bob").
  auto combined_node = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::OR,
      "(x LT 65 76 65 OR x GT 62 6f 62)");
}

TEST_CASE(
    "ASTNode Constructors, tree structure, AND of 2 OR ASTs",
    "[QueryCondition][ast][constructor]") {
  std::vector<int> vals = {0x11111111, 0x22222222, 0x33333333, 0x44444444};

  // Testing a <= 0x11111111, b < 0x22222222, c >= 0x33333333, and d >
  // 0x44444444.
  auto node_val =
      test_value_node("a", &vals[0], QueryConditionOp::LE, "a LE 11 11 11 11");
  auto node_val1 =
      test_value_node("b", &vals[1], QueryConditionOp::LT, "b LT 22 22 22 22");
  auto node_val2 =
      test_value_node("c", &vals[2], QueryConditionOp::GE, "c GE 33 33 33 33");
  auto node_val3 =
      test_value_node("d", &vals[3], QueryConditionOp::GT, "d GT 44 44 44 44");

  // Testing (a <= 0x11111111 OR b < 0x22222222).
  auto node_expr = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::OR,
      "(a LE 11 11 11 11 OR b LT 22 22 22 22)");

  // Testing (c >= 0x33333333 OR d > 0x44444444).
  auto node_expr1 = test_expression_node(
      node_val2,
      node_val3,
      QueryConditionCombinationOp::OR,
      "(c GE 33 33 33 33 OR d GT 44 44 44 44)");

  // Testing ((a <= 0x11111111 OR b < 0x22222222) AND (c >= 0x33333333 OR d >
  // 0x44444444)).
  auto node_combine = test_expression_node(
      node_expr,
      node_expr1,
      QueryConditionCombinationOp::AND,
      "((a LE 11 11 11 11 OR b LT 22 22 22 22) AND (c GE 33 33 33 33 OR d GT "
      "44 44 44 44))");
}

TEST_CASE(
    "ASTNode Constructors, tree structure, OR of 2 AND ASTs",
    "[QueryCondition][ast][constructor]") {
  std::vector<int> vals = {0x11111111, 0x22222222, 0x33333333, 0x44444444};

  // Testing a <= 0x11111111, b < 0x22222222, c >= 0x33333333, and d >
  // 0x44444444.
  auto node_val =
      test_value_node("a", &vals[0], QueryConditionOp::LE, "a LE 11 11 11 11");
  auto node_val1 =
      test_value_node("b", &vals[1], QueryConditionOp::LT, "b LT 22 22 22 22");
  auto node_val2 =
      test_value_node("c", &vals[2], QueryConditionOp::GE, "c GE 33 33 33 33");
  auto node_val3 =
      test_value_node("d", &vals[3], QueryConditionOp::GT, "d GT 44 44 44 44");

  // Testing (a <= 0x11111111 AND b < 0x22222222).
  auto node_expr = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::AND,
      "(a LE 11 11 11 11 AND b LT 22 22 22 22)");

  // Testing (c >= 0x33333333 AND d > 0x44444444).
  auto node_expr1 = test_expression_node(
      node_val2,
      node_val3,
      QueryConditionCombinationOp::AND,
      "(c GE 33 33 33 33 AND d GT 44 44 44 44)");

  // Testing ((a <= 0x11111111 AND b < 0x22222222) OR (c >= 0x33333333 AND d >
  // 0x44444444)).
  auto node_combine = test_expression_node(
      node_expr,
      node_expr1,
      QueryConditionCombinationOp::OR,
      "((a LE 11 11 11 11 AND b LT 22 22 22 22) OR (c GE 33 33 33 33 AND d GT "
      "44 44 44 44))");
}

TEST_CASE(
    "ASTNode Constructors, tree structure, AND of 2 AND ASTs",
    "[QueryCondition][ast][constructor]") {
  std::vector<int> vals = {0x11111111, 0x22222222, 0x33333333, 0x44444444};

  // Testing a <= 0x11111111, b < 0x22222222, c >= 0x33333333, and d >
  // 0x44444444.
  auto node_val =
      test_value_node("a", &vals[0], QueryConditionOp::LE, "a LE 11 11 11 11");
  auto node_val1 =
      test_value_node("b", &vals[1], QueryConditionOp::LT, "b LT 22 22 22 22");
  auto node_val2 =
      test_value_node("c", &vals[2], QueryConditionOp::GE, "c GE 33 33 33 33");
  auto node_val3 =
      test_value_node("d", &vals[3], QueryConditionOp::GT, "d GT 44 44 44 44");

  // Testing (a <= 0x11111111 AND b < 0x22222222).
  auto node_expr = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::AND,
      "(a LE 11 11 11 11 AND b LT 22 22 22 22)");

  // Testing (c >= 0x33333333 AND d > 0x44444444).
  auto node_expr1 = test_expression_node(
      node_val2,
      node_val3,
      QueryConditionCombinationOp::AND,
      "(c GE 33 33 33 33 AND d GT 44 44 44 44)");

  // Testing (a <= 0x11111111 AND b < 0x22222222 AND c >= 0x33333333 AND d >
  // 0x44444444).
  auto node_combine = test_expression_node(
      node_expr,
      node_expr1,
      QueryConditionCombinationOp::AND,
      "(a LE 11 11 11 11 AND b LT 22 22 22 22 AND c GE 33 33 33 33 AND d GT 44 "
      "44 44 44)");
}

TEST_CASE(
    "ASTNode Constructors, tree structure, OR of 2 OR ASTs",
    "[QueryCondition][ast][constructor]") {
  std::vector<int> vals = {0x11111111, 0x22222222, 0x33333333, 0x44444444};

  // Testing a <= 0x11111111, b < 0x22222222, c >= 0x33333333, and d >
  // 0x44444444.
  auto node_val =
      test_value_node("a", &vals[0], QueryConditionOp::LE, "a LE 11 11 11 11");
  auto node_val1 =
      test_value_node("b", &vals[1], QueryConditionOp::LT, "b LT 22 22 22 22");
  auto node_val2 =
      test_value_node("c", &vals[2], QueryConditionOp::GE, "c GE 33 33 33 33");
  auto node_val3 =
      test_value_node("d", &vals[3], QueryConditionOp::GT, "d GT 44 44 44 44");

  // Testing (a <= 0x11111111 OR b < 0x22222222).
  auto node_expr = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::OR,
      "(a LE 11 11 11 11 OR b LT 22 22 22 22)");

  // Testing (c >= 0x33333333 OR d > 0x44444444).
  auto node_expr1 = test_expression_node(
      node_val2,
      node_val3,
      QueryConditionCombinationOp::OR,
      "(c GE 33 33 33 33 OR d GT 44 44 44 44)");

  // Testing (a <= 0x11111111 OR b < 0x22222222 OR c >= 0x33333333 OR d >
  // 0x44444444).
  auto node_combine = test_expression_node(
      node_expr,
      node_expr1,
      QueryConditionCombinationOp::OR,
      "(a LE 11 11 11 11 OR b LT 22 22 22 22 OR c GE 33 33 33 33 OR d GT 44 44 "
      "44 44)");
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
  std::vector<int32_t> vals = {
      0x1aaaaaaa, 0x1bbbbbbb, 0x1ccccccc, 0x1ddddddd, 0x1eeeeeee};
  std::vector<std::string> expected_strs = {
      "foo NE aa aa aa 1a",
      "foo NE bb bb bb 1b",
      "foo NE cc cc cc 1c",
      "foo NE dd dd dd 1d",
      "foo NE ee ee ee 1e"};
  std::vector<tdb_unique_ptr<ASTNode>> ast_val_nodes;

  // Creating the value nodes foo != 0x1aaaaaaa, foo != 0x1bbbbbbb, foo !=
  // 0x1ccccccc, foo != 0x1ddddddd, and foo != 0x1eeeeeee.
  for (size_t i = 0; i < vals.size(); ++i) {
    ast_val_nodes.push_back(test_value_node(
        "foo", &vals[i], QueryConditionOp::NE, expected_strs[i]));
  }

  // Testing (foo != 0x1aaaaaaa AND foo != 0x1bbbbbbb).
  auto combined_and1 = test_expression_node(
      ast_val_nodes[0],
      ast_val_nodes[1],
      QueryConditionCombinationOp::AND,
      "(foo NE aa aa aa 1a AND foo NE bb bb bb 1b)");

  // Testing (foo != 0x1aaaaaaa AND foo != 0x1bbbbbbb AND foo != 0x1ccccccc).
  auto combined_and2 = test_expression_node(
      combined_and1,
      ast_val_nodes[2],
      QueryConditionCombinationOp::AND,
      "(foo NE aa aa aa 1a AND foo NE bb bb bb 1b AND foo NE cc cc cc 1c)");

  // Testing (foo != 0x1aaaaaaa AND foo != 0x1bbbbbbb AND foo != 0x1ccccccc
  // AND foo != 0x1ddddddd).
  auto combined_and3 = test_expression_node(
      combined_and2,
      ast_val_nodes[3],
      QueryConditionCombinationOp::AND,
      "(foo NE aa aa aa 1a AND foo NE bb bb bb 1b AND foo NE cc cc cc 1c AND "
      "foo NE dd dd dd 1d)");

  // Testing (foo != 0x1aaaaaaa AND foo != 0x1bbbbbbb AND foo != 0x1ccccccc
  // AND foo != 0x1ddddddd AND foo != 0x1eeeeeee).
  auto combined_and4 = test_expression_node(
      combined_and3,
      ast_val_nodes[4],
      QueryConditionCombinationOp::AND,
      "(foo NE aa aa aa 1a AND foo NE bb bb bb 1b AND foo NE cc cc cc 1c AND "
      "foo NE dd dd dd 1d AND foo NE ee ee ee 1e)");
}

TEST_CASE(
    "ASTNode Constructors, adding simple clauses to an OR tree",
    "[QueryCondition][ast][constructor]") {
  std::vector<int32_t> vals = {
      0x1aaaaaaa, 0x1bbbbbbb, 0x1ccccccc, 0x1ddddddd, 0x1eeeeeee};
  std::vector<std::string> expected_strs = {
      "foo NE aa aa aa 1a",
      "foo NE bb bb bb 1b",
      "foo NE cc cc cc 1c",
      "foo NE dd dd dd 1d",
      "foo NE ee ee ee 1e"};
  std::vector<tdb_unique_ptr<ASTNode>> ast_val_nodes;

  // Creating the value nodes foo != 0x1aaaaaaa, foo != 0x1bbbbbbb, foo !=
  // 0x1ccccccc, foo != 0x1ddddddd, and foo != 0x1eeeeeee.
  for (size_t i = 0; i < vals.size(); ++i) {
    ast_val_nodes.push_back(test_value_node(
        "foo", &vals[i], QueryConditionOp::NE, expected_strs[i]));
  }

  // Testing (foo != 0x1aaaaaaa OR foo != 0x1bbbbbbb).
  auto combined_or1 = test_expression_node(
      ast_val_nodes[0],
      ast_val_nodes[1],
      QueryConditionCombinationOp::OR,
      "(foo NE aa aa aa 1a OR foo NE bb bb bb 1b)");

  // Testing (foo != 0x1aaaaaaa OR foo != 0x1bbbbbbb OR foo != 0x1ccccccc).
  auto combined_or2 = test_expression_node(
      combined_or1,
      ast_val_nodes[2],
      QueryConditionCombinationOp::OR,
      "(foo NE aa aa aa 1a OR foo NE bb bb bb 1b OR foo NE cc cc cc 1c)");

  // Testing (foo != 0x1aaaaaaa OR foo != 0x1bbbbbbb OR foo != 0x1ccccccc
  // OR foo != 0x1ddddddd).
  auto combined_or3 = test_expression_node(
      combined_or2,
      ast_val_nodes[3],
      QueryConditionCombinationOp::OR,
      "(foo NE aa aa aa 1a OR foo NE bb bb bb 1b OR foo NE cc cc cc 1c OR "
      "foo NE dd dd dd 1d)");

  // Testing (foo != 0x1aaaaaaa AND foo != 0x1bbbbbbb AND foo != 0x1ccccccc
  // AND foo != 0x1ddddddd AND foo != 0x1eeeeeee).
  auto combined_or4 = test_expression_node(
      combined_or3,
      ast_val_nodes[4],
      QueryConditionCombinationOp::OR,
      "(foo NE aa aa aa 1a OR foo NE bb bb bb 1b OR foo NE cc cc cc 1c OR "
      "foo NE dd dd dd 1d OR foo NE ee ee ee 1e)");
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

TEST_CASE("ASTNode negate, string", "[QueryCondition][ast][negate]") {
  // Test validity of construction of value node.
  char value[] = "bar";
  auto node_val = test_string_value_node(
      "foo",
      static_cast<char*>(value),
      QueryConditionOp::LE,
      "foo GT 62 61 72",
      true);
}

TEST_CASE(
    "ASTNode negate, basic AND combine", "[QueryCondition][ast][negate]") {
  // Testing (x < 0xabcdef12).
  int val = 0xabcdef12;
  auto node_val = test_value_node(
      "x", &val, QueryConditionOp::LT, "x GE 12 ef cd ab", true);

  // Testing (y > 0x33333333).
  int val1 = 0x33333333;
  auto node_val1 = test_value_node(
      "y", &val1, QueryConditionOp::GT, "y LE 33 33 33 33", true);

  // Testing (x < 0xabcdef12 AND y > 0x33333333).
  auto combined_node = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::AND,
      "(x GE 12 ef cd ab OR y LE 33 33 33 33)",
      true);
}

TEST_CASE("ASTNode negate, basic OR combine", "[QueryCondition][ast][negate]") {
  // Testing (x < 0xabcdef12).
  int val = 0xabcdef12;
  auto node_val = test_value_node(
      "x", &val, QueryConditionOp::LT, "x GE 12 ef cd ab", true);

  // Testing (y > 0x33333333).
  int val1 = 0x33333333;
  auto node_val1 = test_value_node(
      "y", &val1, QueryConditionOp::GT, "y LE 33 33 33 33", true);

  // Testing (x < 0xabcdef12 AND y > 0x33333333).
  auto combined_node = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::OR,
      "(x GE 12 ef cd ab AND y LE 33 33 33 33)",
      true);
}

TEST_CASE(
    "ASTNode negate, basic AND combine, strings",
    "[QueryCondition][ast][negate]") {
  // Testing (x < "eve").
  char e[] = "eve";
  auto node_val = test_string_value_node(
      "x", e, QueryConditionOp::LT, "x GE 65 76 65", true);

  // Testing (x > "bob").
  char b[] = "bob";
  auto node_val1 = test_string_value_node(
      "x", b, QueryConditionOp::GT, "x LE 62 6f 62", true);

  // Testing (x < "eve" AND x > "bob").
  auto combined_node = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::AND,
      "(x GE 65 76 65 OR x LE 62 6f 62)",
      true);
}

TEST_CASE(
    "ASTNode negate, basic OR combine, strings",
    "[QueryCondition][ast][negate]") {
  // Testing (x < "eve").
  char e[] = "eve";
  auto node_val = test_string_value_node(
      "x", e, QueryConditionOp::LT, "x GE 65 76 65", true);

  // Testing (x > "bob").
  char b[] = "bob";
  auto node_val1 = test_string_value_node(
      "x", b, QueryConditionOp::GT, "x LE 62 6f 62", true);

  // Testing (x < "eve" OR x > "bob").
  auto combined_node = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::OR,
      "(x GE 65 76 65 AND x LE 62 6f 62)",
      true);
}

TEST_CASE(
    "ASTNode negate, tree structure, AND of 2 OR ASTs",
    "[QueryCondition][ast][negate]") {
  std::vector<int> vals = {0x11111111, 0x22222222, 0x33333333, 0x44444444};

  // Testing a <= 0x11111111, b < 0x22222222, c >= 0x33333333, and d >
  // 0x44444444.
  auto node_val = test_value_node(
      "a", &vals[0], QueryConditionOp::LE, "a GT 11 11 11 11", true);
  auto node_val1 = test_value_node(
      "b", &vals[1], QueryConditionOp::LT, "b GE 22 22 22 22", true);
  auto node_val2 = test_value_node(
      "c", &vals[2], QueryConditionOp::GE, "c LT 33 33 33 33", true);
  auto node_val3 = test_value_node(
      "d", &vals[3], QueryConditionOp::GT, "d LE 44 44 44 44", true);

  // Testing (a <= 0x11111111 OR b < 0x22222222).
  auto node_expr = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::OR,
      "(a GT 11 11 11 11 AND b GE 22 22 22 22)",
      true);

  // Testing (c >= 0x33333333 OR d > 0x44444444).
  auto node_expr1 = test_expression_node(
      node_val2,
      node_val3,
      QueryConditionCombinationOp::OR,
      "(c LT 33 33 33 33 AND d LE 44 44 44 44)",
      true);

  // Testing ((a <= 0x11111111 OR b < 0x22222222) AND (c >= 0x33333333 OR d >
  // 0x44444444)).
  auto node_combine = test_expression_node(
      node_expr,
      node_expr1,
      QueryConditionCombinationOp::AND,
      "((a GT 11 11 11 11 AND b GE 22 22 22 22) OR (c LT 33 33 33 33 AND d LE "
      "44 44 44 44))",
      true);
}

TEST_CASE(
    "ASTNode negate, tree structure, OR of 2 AND ASTs",
    "[QueryCondition][ast][negate]") {
  std::vector<int> vals = {0x11111111, 0x22222222, 0x33333333, 0x44444444};

  // Testing a <= 0x11111111, b < 0x22222222, c >= 0x33333333, and d >
  // 0x44444444.
  auto node_val = test_value_node(
      "a", &vals[0], QueryConditionOp::LE, "a GT 11 11 11 11", true);
  auto node_val1 = test_value_node(
      "b", &vals[1], QueryConditionOp::LT, "b GE 22 22 22 22", true);
  auto node_val2 = test_value_node(
      "c", &vals[2], QueryConditionOp::GE, "c LT 33 33 33 33", true);
  auto node_val3 = test_value_node(
      "d", &vals[3], QueryConditionOp::GT, "d LE 44 44 44 44", true);

  // Testing (a <= 0x11111111 AND b < 0x22222222).
  auto node_expr = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::AND,
      "(a GT 11 11 11 11 OR b GE 22 22 22 22)",
      true);

  // Testing (c >= 0x33333333 AND d > 0x44444444).
  auto node_expr1 = test_expression_node(
      node_val2,
      node_val3,
      QueryConditionCombinationOp::AND,
      "(c LT 33 33 33 33 OR d LE 44 44 44 44)",
      true);

  // Testing ((a <= 0x11111111 AND b < 0x22222222) OR (c >= 0x33333333 AND d >
  // 0x44444444)).
  auto node_combine = test_expression_node(
      node_expr,
      node_expr1,
      QueryConditionCombinationOp::OR,
      "((a GT 11 11 11 11 OR b GE 22 22 22 22) AND (c LT 33 33 33 33 OR d LE "
      "44 44 44 44))",
      true);
}

TEST_CASE(
    "ASTNode negate, tree structure, AND of 2 AND ASTs",
    "[QueryCondition][ast][negate]") {
  std::vector<int> vals = {0x11111111, 0x22222222, 0x33333333, 0x44444444};

  // Testing a <= 0x11111111, b < 0x22222222, c >= 0x33333333, and d >
  // 0x44444444.
  auto node_val = test_value_node(
      "a", &vals[0], QueryConditionOp::LE, "a GT 11 11 11 11", true);
  auto node_val1 = test_value_node(
      "b", &vals[1], QueryConditionOp::LT, "b GE 22 22 22 22", true);
  auto node_val2 = test_value_node(
      "c", &vals[2], QueryConditionOp::GE, "c LT 33 33 33 33", true);
  auto node_val3 = test_value_node(
      "d", &vals[3], QueryConditionOp::GT, "d LE 44 44 44 44", true);

  // Testing (a <= 0x11111111 AND b < 0x22222222).
  auto node_expr = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::AND,
      "(a GT 11 11 11 11 OR b GE 22 22 22 22)",
      true);

  // Testing (c >= 0x33333333 AND d > 0x44444444).
  auto node_expr1 = test_expression_node(
      node_val2,
      node_val3,
      QueryConditionCombinationOp::AND,
      "(c LT 33 33 33 33 OR d LE 44 44 44 44)",
      true);

  // Testing (a <= 0x11111111 AND b < 0x22222222 AND c >= 0x33333333 AND d >
  // 0x44444444).
  auto node_combine = test_expression_node(
      node_expr,
      node_expr1,
      QueryConditionCombinationOp::AND,
      "(a GT 11 11 11 11 OR b GE 22 22 22 22 OR c LT 33 33 33 33 OR d LE 44 "
      "44 44 44)",
      true);
}

TEST_CASE(
    "ASTNode negate, tree structure, OR of 2 OR ASTs",
    "[QueryCondition][ast][negate]") {
  std::vector<int> vals = {0x11111111, 0x22222222, 0x33333333, 0x44444444};

  // Testing a <= 0x11111111, b < 0x22222222, c >= 0x33333333, and d >
  // 0x44444444.
  auto node_val = test_value_node(
      "a", &vals[0], QueryConditionOp::LE, "a GT 11 11 11 11", true);
  auto node_val1 = test_value_node(
      "b", &vals[1], QueryConditionOp::LT, "b GE 22 22 22 22", true);
  auto node_val2 = test_value_node(
      "c", &vals[2], QueryConditionOp::GE, "c LT 33 33 33 33", true);
  auto node_val3 = test_value_node(
      "d", &vals[3], QueryConditionOp::GT, "d LE 44 44 44 44", true);

  // Testing (a <= 0x11111111 OR b < 0x22222222).
  auto node_expr = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::OR,
      "(a GT 11 11 11 11 AND b GE 22 22 22 22)",
      true);

  // Testing (c >= 0x33333333 OR d > 0x44444444).
  auto node_expr1 = test_expression_node(
      node_val2,
      node_val3,
      QueryConditionCombinationOp::OR,
      "(c LT 33 33 33 33 AND d LE 44 44 44 44)",
      true);

  // Testing (a <= 0x11111111 OR b < 0x22222222 OR c >= 0x33333333 OR d >
  // 0x44444444).
  auto node_combine = test_expression_node(
      node_expr,
      node_expr1,
      QueryConditionCombinationOp::OR,
      "(a GT 11 11 11 11 AND b GE 22 22 22 22 AND c LT 33 33 33 33 AND d LE 44 "
      "44 "
      "44 44)",
      true);
}

TEST_CASE("ASTNode negate, complex tree", "[QueryCondition][ast][negate]") {
  // Testing (x < 5).
  int val = 5;
  auto node_val = test_value_node(
      "x", &val, QueryConditionOp::LT, "x GE 05 00 00 00", true);

  // Testing (y > 3).
  int val1 = 3;
  auto node_val1 = test_value_node(
      "y", &val1, QueryConditionOp::GT, "y LE 03 00 00 00", true);

  // Testing (x < 5 AND y > 3).
  auto combined_node = test_expression_node(
      node_val,
      node_val1,
      QueryConditionCombinationOp::AND,
      "(x GE 05 00 00 00 OR y LE 03 00 00 00)",
      true);

  // Testing (a = 23).
  int val2 = 23;
  auto node_val2 = test_value_node(
      "a", &val2, QueryConditionOp::EQ, "a NE 17 00 00 00", true);

  // Testing (b != 2);
  int val3 = 2;
  auto node_val3 = test_value_node(
      "b", &val3, QueryConditionOp::NE, "b EQ 02 00 00 00", true);

  // Testing (c <= 8).
  int val4 = 8;
  auto node_val4 = test_value_node(
      "c", &val4, QueryConditionOp::LE, "c GT 08 00 00 00", true);

  // Testing (a = 23 OR b != 2 OR c <= 8).
  auto combined_node_inter1 = test_expression_node(
      node_val2,
      node_val3,
      QueryConditionCombinationOp::OR,
      "(a NE 17 00 00 00 AND b EQ 02 00 00 00)",
      true);
  auto combined_node1 = test_expression_node(
      combined_node_inter1,
      node_val4,
      QueryConditionCombinationOp::OR,
      "(a NE 17 00 00 00 AND b EQ 02 00 00 00 AND c GT 08 00 00 00)",
      true);

  // Testing ((x < 5 AND y > 3) OR a = 23 OR b != 2 OR c <= 8).
  test_expression_node(
      combined_node,
      combined_node1,
      QueryConditionCombinationOp::OR,
      "((x GE 05 00 00 00 OR y LE 03 00 00 00) AND a NE 17 00 00 00 AND b EQ "
      "02 "
      "00 00 00 AND c GT 08 00 00 00)",
      true);
}

TEST_CASE(
    "ASTNode negate, adding simple clauses to an AND tree",
    "[QueryCondition][ast][negate]") {
  std::vector<int32_t> vals = {
      0x1aaaaaaa, 0x1bbbbbbb, 0x1ccccccc, 0x1ddddddd, 0x1eeeeeee};
  std::vector<std::string> expected_strs = {
      "foo NE aa aa aa 1a",
      "foo NE bb bb bb 1b",
      "foo NE cc cc cc 1c",
      "foo NE dd dd dd 1d",
      "foo NE ee ee ee 1e"};
  std::vector<tdb_unique_ptr<ASTNode>> ast_val_nodes;

  // Creating the value nodes foo != 0x1aaaaaaa, foo != 0x1bbbbbbb, foo !=
  // 0x1ccccccc, foo != 0x1ddddddd, and foo != 0x1eeeeeee.
  for (size_t i = 0; i < vals.size(); ++i) {
    ast_val_nodes.push_back(test_value_node(
        "foo", &vals[i], QueryConditionOp::NE, expected_strs[i]));
  }

  // Testing (foo != 0x1aaaaaaa AND foo != 0x1bbbbbbb).
  auto combined_and1 = test_expression_node(
      ast_val_nodes[0],
      ast_val_nodes[1],
      QueryConditionCombinationOp::AND,
      "(foo EQ aa aa aa 1a OR foo EQ bb bb bb 1b)",
      true);

  // Testing (foo != 0x1aaaaaaa AND foo != 0x1bbbbbbb AND foo != 0x1ccccccc).
  auto combined_and2 = test_expression_node(
      combined_and1,
      ast_val_nodes[2],
      QueryConditionCombinationOp::AND,
      "(foo EQ aa aa aa 1a OR foo EQ bb bb bb 1b OR foo EQ cc cc cc 1c)",
      true);

  // Testing (foo != 0x1aaaaaaa AND foo != 0x1bbbbbbb AND foo != 0x1ccccccc
  // AND foo != 0x1ddddddd).
  auto combined_and3 = test_expression_node(
      combined_and2,
      ast_val_nodes[3],
      QueryConditionCombinationOp::AND,
      "(foo EQ aa aa aa 1a OR foo EQ bb bb bb 1b OR foo EQ cc cc cc 1c OR "
      "foo EQ dd dd dd 1d)",
      true);

  // Testing (foo != 0x1aaaaaaa AND foo != 0x1bbbbbbb AND foo != 0x1ccccccc
  // AND foo != 0x1ddddddd AND foo != 0x1eeeeeee).
  auto combined_and4 = test_expression_node(
      combined_and3,
      ast_val_nodes[4],
      QueryConditionCombinationOp::AND,
      "(foo EQ aa aa aa 1a OR foo EQ bb bb bb 1b OR foo EQ cc cc cc 1c OR "
      "foo EQ dd dd dd 1d OR foo EQ ee ee ee 1e)",
      true);
}

TEST_CASE(
    "ASTNode negate, adding simple clauses to an OR tree",
    "[QueryCondition][ast][negate]") {
  std::vector<int32_t> vals = {
      0x1aaaaaaa, 0x1bbbbbbb, 0x1ccccccc, 0x1ddddddd, 0x1eeeeeee};
  std::vector<std::string> expected_strs = {
      "foo NE aa aa aa 1a",
      "foo NE bb bb bb 1b",
      "foo NE cc cc cc 1c",
      "foo NE dd dd dd 1d",
      "foo NE ee ee ee 1e"};
  std::vector<tdb_unique_ptr<ASTNode>> ast_val_nodes;

  // Creating the value nodes foo != 0x1aaaaaaa, foo != 0x1bbbbbbb, foo !=
  // 0x1ccccccc, foo != 0x1ddddddd, and foo != 0x1eeeeeee.
  for (size_t i = 0; i < vals.size(); ++i) {
    ast_val_nodes.push_back(test_value_node(
        "foo", &vals[i], QueryConditionOp::NE, expected_strs[i]));
  }

  // Testing (foo != 0x1aaaaaaa OR foo != 0x1bbbbbbb).
  auto combined_or1 = test_expression_node(
      ast_val_nodes[0],
      ast_val_nodes[1],
      QueryConditionCombinationOp::OR,
      "(foo EQ aa aa aa 1a AND foo EQ bb bb bb 1b)",
      true);

  // Testing (foo != 0x1aaaaaaa OR foo != 0x1bbbbbbb OR foo != 0x1ccccccc).
  auto combined_or2 = test_expression_node(
      combined_or1,
      ast_val_nodes[2],
      QueryConditionCombinationOp::OR,
      "(foo EQ aa aa aa 1a AND foo EQ bb bb bb 1b AND foo EQ cc cc cc 1c)",
      true);

  // Testing (foo != 0x1aaaaaaa OR foo != 0x1bbbbbbb OR foo != 0x1ccccccc
  // OR foo != 0x1ddddddd).
  auto combined_or3 = test_expression_node(
      combined_or2,
      ast_val_nodes[3],
      QueryConditionCombinationOp::OR,
      "(foo EQ aa aa aa 1a AND foo EQ bb bb bb 1b AND foo EQ cc cc cc 1c AND "
      "foo EQ dd dd dd 1d)",
      true);

  // Testing (foo != 0x1aaaaaaa AND foo != 0x1bbbbbbb AND foo != 0x1ccccccc
  // AND foo != 0x1ddddddd AND foo != 0x1eeeeeee).
  auto combined_or4 = test_expression_node(
      combined_or3,
      ast_val_nodes[4],
      QueryConditionCombinationOp::OR,
      "(foo EQ aa aa aa 1a AND foo EQ bb bb bb 1b AND foo EQ cc cc cc 1c AND "
      "foo EQ dd dd dd 1d AND foo EQ ee ee ee 1e)",
      true);
}

TEST_CASE(
    "ASTNode negate, complex tree with depth > 2",
    "[QueryCondition][ast][negate]") {
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

  auto x_neq_six = test_value_node(
      "x", &vals[5], QueryConditionOp::NE, "x EQ 06 00 00 00", true);
  auto one_or_two = test_expression_node(
      ast_value_vector[0],
      ast_value_vector[1],
      QueryConditionCombinationOp::OR,
      "(x NE 01 00 00 00 AND x NE 02 00 00 00)",
      true);
  auto three_or_four = test_expression_node(
      ast_value_vector[2],
      ast_value_vector[3],
      QueryConditionCombinationOp::OR,
      "(x NE 03 00 00 00 AND x NE 04 00 00 00)",
      true);
  auto six_or_seven = test_expression_node(
      ast_value_vector[5],
      ast_value_vector[6],
      QueryConditionCombinationOp::OR,
      "(x NE 06 00 00 00 AND x NE 07 00 00 00)",
      true);
  auto eight_and_nine = test_expression_node(
      ast_value_vector[7],
      ast_value_vector[8],
      QueryConditionCombinationOp::AND,
      "(x EQ 08 00 00 00 OR x EQ 09 00 00 00)",
      true);

  auto subtree_a = test_expression_node(
      one_or_two,
      three_or_four,
      QueryConditionCombinationOp::AND,
      "((x NE 01 00 00 00 AND x NE 02 00 00 00) OR (x NE 03 00 00 00 AND x NE "
      "04 00 00 00))",
      true);
  auto subtree_d = test_expression_node(
      eight_and_nine,
      six_or_seven,
      QueryConditionCombinationOp::AND,
      "(x EQ 08 00 00 00 OR x EQ 09 00 00 00 OR (x NE 06 00 00 00 AND x NE 07 "
      "00 00 00))",
      true);
  auto subtree_c = test_expression_node(
      subtree_d,
      ast_value_vector[4],
      QueryConditionCombinationOp::OR,
      "((x EQ 08 00 00 00 OR x EQ 09 00 00 00 OR (x NE 06 00 00 00 AND x NE "
      "07 00 00 00)) AND x NE 05 00 00 00)",
      true);
  auto subtree_b = test_expression_node(
      subtree_c,
      x_neq_six,
      QueryConditionCombinationOp::AND,
      "(((x EQ 08 00 00 00 OR x EQ 09 00 00 00 OR (x NE 06 00 00 00 AND x NE "
      "07 00 00 00)) AND x NE 05 00 00 00) OR x EQ 06 00 00 00)",
      true);

  auto tree = test_expression_node(
      subtree_a,
      subtree_b,
      QueryConditionCombinationOp::OR,
      "(((x NE 01 00 00 00 AND x NE 02 00 00 00) OR (x NE 03 00 00 00 AND x NE "
      "04 00 00 00)) AND (((x EQ 08 00 00 00 OR x EQ 09 00 00 00 OR (x NE 06 "
      "00 00 00 AND x NE 07 00 00 00)) AND x NE 05 00 00 00) OR x EQ 06 00 00 "
      "00))",
      true);
}
