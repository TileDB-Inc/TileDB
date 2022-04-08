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

#include "test/src/helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/query/query_ast.h"

#include "catch.hpp"

using namespace tiledb::sm;

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
      ast_combine(node_val, node_val1, QueryConditionCombinationOp::AND);
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
      ast_combine(node_val2, node_val3, QueryConditionCombinationOp::OR);
  auto combined_node1 =
      ast_combine(node_inter1, node_val4, QueryConditionCombinationOp::OR);
  REQUIRE(
      ast_node_to_str(combined_node1) ==
      "(a EQ 17 00 00 00 OR b NE 02 00 00 00 OR c LE 08 00 00 00)");

  auto combined_node2 = ast_combine(
      combined_node, combined_node1, QueryConditionCombinationOp::OR);
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