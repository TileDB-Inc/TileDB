/**
 * @file   expression_tree.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file defines classes ExpressionNode and ExpressionTree.
 */

#include <inttypes.h>
#include <string>
#include <set>

/** 
 * This class implements a node of an expression tree. A node may contain a
 * constant, a variable, or an operator. Currently only double constants 
 * supported.
 */
class ExpressionNode {
  // TYPE DEFINITIONS

 public:
  // CONSTRUCTORS AND DESTRUCTORS
  /** Simple constructor. */
  ExpressionNode();
  // TODO: overload constructor to capture all cases.
  /** Destructor that deletes the entire subtree under this node. */
  ~ExpressionNode();

 private:
  // PROTECTED ATTRIBUTES
  double v_double_;
  /** The variable name. */
  std::string var_;
  /** The operator. */
  std::string op_;
  /** The left child of this node in the expression tree. */
  ExpressionNode* left_;
  /** The right child of this node in the expression tree. */
  ExpressionNode* right_;
};

/** 
 * An expression tree, used to represent and evaluate mathematical 
 * expressions. 
 */
class ExpressionTree {
 public:

 private:
  /** The tree root. */
  ExpressionNode* root;
  /** The names of the variables included in the tree. */
  std::set<std::string> vars;
};
