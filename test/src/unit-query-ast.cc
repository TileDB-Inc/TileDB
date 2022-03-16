/**
 * @file unit-query-ast.cc
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
 * Tests the `ASTNode`, `ASTNodeVal` and `ASTNodeExpr` classes.
 */

#include "tiledb/common/common.h"
#include "tiledb/sm/query/query_ast.h"

#include "catch.hpp"

using namespace tiledb::sm;

TEST_CASE("ASTNode Constructors", "[QueryCondition][ast][constructor]") {
  // ASTNodeVal constructor
  std::string field_name = "x";
  int val = 5;
  auto node_val = make_shared<ASTNodeVal>(
      HERE(), field_name, &val, sizeof(val), QueryConditionOp::LT);
  REQUIRE(node_val->to_str() == "x LT 05 00 00 00");

  // ASTNodeVal constructor
  std::string field_name1 = "y";
  int val1 = 3;
  auto node_val1 = make_shared<ASTNodeVal>(
      HERE(), field_name1, &val1, sizeof(val1), QueryConditionOp::GT);
  REQUIRE(node_val1->to_str() == "y GT 03 00 00 00");

  // ASTNodeExpr constructor
  std::vector<shared_ptr<ASTNode>> vec;
  vec.push_back(node_val);
  vec.push_back(node_val1);
  auto combined_node =
      make_shared<ASTNodeExpr>(HERE(), vec, QueryConditionCombinationOp::AND);
  REQUIRE(combined_node->to_str() == "(x LT 05 00 00 00 AND y GT 03 00 00 00)");

  // ASTNodeVal constructor
  std::string field_name2 = "a";
  int val2 = 23;
  auto node_val2 = make_shared<ASTNodeVal>(
      HERE(), field_name2, &val2, sizeof(val2), QueryConditionOp::EQ);
  REQUIRE(node_val2->to_str() == "a EQ 17 00 00 00");

  // ASTNodeVal constructor
  std::string field_name3 = "b";
  int val3 = 2;
  auto node_val3 = make_shared<ASTNodeVal>(
      HERE(), field_name3, &val3, sizeof(val3), QueryConditionOp::NE);
  REQUIRE(node_val3->to_str() == "b NE 02 00 00 00");

  std::string field_name4 = "c";
  int val4 = 8;
  auto node_val4 = make_shared<ASTNodeVal>(
      HERE(), field_name4, &val4, sizeof(val4), QueryConditionOp::LE);
  REQUIRE(node_val4->to_str() == "c LE 08 00 00 00");

  std::vector<shared_ptr<ASTNode>> vec1;
  vec1.push_back(node_val2);
  vec1.push_back(node_val3);
  vec1.push_back(node_val4);
  auto combined_node1 =
      make_shared<ASTNodeExpr>(HERE(), vec1, QueryConditionCombinationOp::OR);
  REQUIRE(
      combined_node1->to_str() ==
      "(a EQ 17 00 00 00 OR b NE 02 00 00 00 OR c LE 08 00 00 00)");

  std::vector<shared_ptr<ASTNode>> vec2;
  vec2.push_back(combined_node);
  vec2.push_back(combined_node1);
  auto combined_node2 =
      make_shared<ASTNodeExpr>(HERE(), vec2, QueryConditionCombinationOp::OR);
  REQUIRE(
      combined_node2->to_str() ==
      "((x LT 05 00 00 00 AND y GT 03 00 00 00) OR (a EQ 17 00 00 00 OR b NE "
      "02 00 00 00 OR c LE 08 00 00 00))");
}