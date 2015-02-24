/**
 * @file   expression_tree.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file implements the ExpressionNode and ExpressionTree classes.
 */

#include "expression_tree.h"
#include <assert.h>

/* ----------------- ExpressionNode ---------------- */

/******************************************************
************ CONSTRUCTORS AND DESTRUCTORS *************
******************************************************/

ExpressionNode::ExpressionNode() {
  left_ = NULL;
  right_ = NULL;
  op_ = NO_OP;
}

ExpressionNode::ExpressionNode(double const_value, 
                               ExpressionNode* left, 
                               ExpressionNode* right) {
  op_ = NO_OP;
  const_value_ = const_value;
  left_ = left;
  right_ = right;
}

ExpressionNode::ExpressionNode(Operator op, 
                               ExpressionNode* left, 
                               ExpressionNode* right) {
  op_ = op;
  left_ = left;
  right_ = right;
}

ExpressionNode::ExpressionNode(const std::string& var, 
                               ExpressionNode* left, 
                               ExpressionNode* right) {
  op_ = NO_OP;
  var_ = var;
  left_ = left;
  right_ = right;
}

ExpressionNode::~ExpressionNode() {
  if(left_ != NULL)
    delete left_;
  if(right_ != NULL)
    delete right_;
}

/******************************************************
******************** TREE METHODS *********************
******************************************************/

double ExpressionNode::evaluate(const std::map<std::string, 
                                double>& var_values) const {
  // If the node contains a constant, return its value
  if(var_ == "" && op_ == NO_OP) {
    assert(left_ == NULL && right_ == NULL);

    return const_value_;
  // If the node contains a variable, return its value 
  } else if(var_ != "") {
    assert(left_ == NULL && right_ == NULL);
    assert(op_ == NO_OP);

    std::map<std::string, double>::const_iterator it = var_values.find(var_);
    assert(it != var_values.end());

    return it->second;
  // The node contains an operator
  } else {
    assert(op_ != NO_OP);
    assert(left_ != NULL); // At least one child must exist
    
    // Retrieve left and right results
    double left_result, right_result;
    left_result = left_->evaluate(var_values);
    if(right_ != NULL)
      right_result = right_->evaluate(var_values);

    // Return final results based on the operator
    if(op_ == ADD)
      return left_result + right_result;
    else if(op_ == SUB)
      return left_result - right_result;
    else if(op_ == MUL)
      return left_result * right_result;
    else if(op_ == DIV)
      return left_result / right_result;
    else if(op_ == MOD)
      return int64_t(left_result) % int64_t(right_result);
    else if(op_ == GT)
      return left_result > right_result;
    else if(op_ == ST)
      return left_result < right_result;
    else if(op_  == EQ)
      return left_result == right_result;
    else if(op_ == GTEQ)
      return left_result >= right_result;
    else if(op_ == STEQ)
      return left_result <= right_result;
    else if(op_ == AND)
      return left_result && right_result;
    else if(op_ == OR)
      return left_result || right_result;
  }

  // The program will never reach here
  return 0;
}

std::set<std::string> ExpressionNode::gather_vars() const {
  std::set<std::string> vars;

  // If this node contains a variable
  if(var_ != "") {
    // This must be a leaf
    assert(left_ == NULL && right_ == NULL);
    // Insert variable in the set
    vars.insert(var_); 
  } else { // This node is an operator or a constant.
    // If it is a constant (op_ == NO_OP), do nothing
    // If it is an operator, recurse over the children
    if(op_ != NO_OP) {
      assert(left_ != NULL); // The left child must always exist.
      // Retrieve the set of variables in the left subtree
      vars = left_->gather_vars();

      // Retrieve the set of variables in the left subtree
      std::set<std::string> right_vars;
      // If the operator is unary, the right child will be missing.
      if(right_ != NULL) 
        right_vars = right_->gather_vars();

      vars.insert(right_vars.begin(), right_vars.end());
    }
  }

  return vars;
}

void ExpressionNode::insert_left(ExpressionNode* node) {
  assert(left_ == NULL);
  left_ = node;
}

void ExpressionNode::insert_right(ExpressionNode* node) {
  assert(right_ == NULL);
  right_ = node;
}

/* ----------------- ExpressionTree ---------------- */

/******************************************************
************ CONSTRUCTORS AND DESTRUCTORS *************
******************************************************/

ExpressionTree::ExpressionTree(ExpressionNode* root) {
  root_ = root;
  vars_ = root_->gather_vars();
}

ExpressionTree::~ExpressionTree() {
  delete root_;
}

/******************************************************
******************** TREE METHODS *********************
******************************************************/

double ExpressionTree::evaluate(const std::map<std::string, 
                                double>& var_values) const {
  return root_->evaluate(var_values);
}
