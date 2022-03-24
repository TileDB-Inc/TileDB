/**
 * @file query_ast.cc
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
 * Implements Query AST classes.
 */

#include "query_ast.h"

namespace tiledb {
namespace sm {

std::unique_ptr<ASTNodeExpr> ast_combine(
    const std::unique_ptr<ASTNode>& lhs,
    const std::unique_ptr<ASTNode>& rhs,
    QueryConditionCombinationOp combination_op) {
  // AST Construction
  std::vector<std::unique_ptr<ASTNode>> ast_nodes;
  if (lhs->get_tag() == ASTNodeTag::VAL) {
    if (rhs->get_tag() == ASTNodeTag::VAL) {
      ast_nodes.push_back(lhs->clone());
      ast_nodes.push_back(rhs->clone());
    } else {
      // rhs is expression, lhs is value
      auto rhs_tree_expr = dynamic_cast<ASTNodeExpr*>(rhs.get());
      ast_nodes.push_back(lhs->clone());
      for (const auto& elem : rhs_tree_expr->nodes_) {
        ast_nodes.push_back(elem->clone());
      }
    }
  } else if (rhs->get_tag() == ASTNodeTag::VAL) {
    // lhs is expression, rhs is value
    auto lhs_tree_expr = dynamic_cast<ASTNodeExpr*>(lhs.get());
    for (const auto& elem : lhs_tree_expr->nodes_) {
      ast_nodes.push_back(elem->clone());
    }
    ast_nodes.push_back(rhs->clone());
  } else {
    // Both trees are expression trees
    auto lhs_tree_expr = dynamic_cast<ASTNodeExpr*>(lhs.get());
    auto rhs_tree_expr = dynamic_cast<ASTNodeExpr*>(rhs.get());

    if (combination_op == lhs_tree_expr->combination_op_ &&
        lhs_tree_expr->combination_op_ == rhs_tree_expr->combination_op_) {
      // same op
      for (const auto& elem : lhs_tree_expr->nodes_) {
        ast_nodes.push_back(elem->clone());
      }
      for (const auto& elem : rhs_tree_expr->nodes_) {
        ast_nodes.push_back(elem->clone());
      }
    } else if (combination_op == lhs_tree_expr->combination_op_) {
      for (const auto& elem : lhs_tree_expr->nodes_) {
        ast_nodes.push_back(elem->clone());
      }
      ast_nodes.push_back(rhs->clone());
    } else if (combination_op == rhs_tree_expr->combination_op_) {
      ast_nodes.push_back(lhs->clone());
      for (const auto& elem : rhs_tree_expr->nodes_) {
        ast_nodes.push_back(elem->clone());
      }
    } else {
      ast_nodes.push_back(lhs->clone());
      ast_nodes.push_back(rhs->clone());
    }
  }
  return std::make_unique<ASTNodeExpr>(std::move(ast_nodes), combination_op);
}

}  // namespace sm
}  // namespace tiledb