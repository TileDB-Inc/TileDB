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
#include <map>

/** 
 * This class implements a node of an expression tree. A node may contain a
 * constant, a variable, or an operator. Currently only double constants 
 * supported.
 */
class ExpressionNode {
 public:
  // TYPE DEFINITIONS
  /** 
   * Supported operators:
   *
   * 1. ADD: Addition operator '+'.
   * 
   * 2. SUB: Substraction operator '-'.
   * 
   * 3. MUL: Multiplication operator '*'.
   * 
   * 4. DIV: (Double) Division operator '/'.
   * 
   * 5. MOD: Modulo operator '%'.
   * 
   * 6. GT: Greater than operator '>'.
   * 
   * 7. ST: Smaller than operator '<'.
   * 
   * 8. EQ: Equality operator '=='.
   * 
   * 9. GTEQ: Greater than or equal operator '>='.
   * 
   * 10. STEQ: Smaller than or equal operator '<='.
   * 
   * 11. AND: Logical AND operator '&&'.
   * 
   * 12. OR: Logical OR operator '||'.
   * 
   * 13. NO_OP: Special value indicating that this node is not an operator.
   */
  enum Operator {ADD, SUB, MUL, DIV, MOD, GT, ST, EQ, GTEQ, STEQ, 
                 AND, OR, NO_OP}; 

  // CONSTRUCTORS AND DESTRUCTORS
  /** Simple constructor. */
  ExpressionNode();
  /** Initialization of constant value. */
  ExpressionNode(double const_value, 
                 ExpressionNode* left = NULL, 
                 ExpressionNode* right = NULL);
  /** Initialization of operator. */
  ExpressionNode(Operator op, 
                 ExpressionNode* left = NULL, 
                 ExpressionNode* right = NULL);
  /** Initialization of variable. */
  ExpressionNode(const std::string& var, 
                 ExpressionNode* left = NULL, 
                 ExpressionNode* right = NULL);
  /** Destructor that deletes the entire subtree under this node. */
  ~ExpressionNode();

  // TREE METHODS
  /** 
   * It evaluates the expression represented by its subtree, substituting the
   * variables with their respective values provided in the input. 
   */
  double evaluate(const std::map<std::string, double>& var_values) const;
  /** 
   * Returns the set of variables included in the subtree root at
   * this node.
   */
  std::set<std::string> gather_vars() const;
  /** 
   * Assigns the input node as the left child of the node. Note that
   * the left child must be NULL in order for the insertion to take
   * place. */
  void insert_left(ExpressionNode* node);
  /** 
   * Assigns the input node as the right child of the node. Note that
   * the right child must be NULL in order for the insertion to take
   * place. */
  void insert_right(ExpressionNode* node);

 private:
  // PROTECTED ATTRIBUTES
  /** Constant value. */
  double const_value_;
  /** The variable name. */
  std::string var_;
  /** The operator. */
  Operator op_;
  /** The left child of this node in the expression tree. */
  ExpressionNode* left_;
  /** The right child of this node in the expression tree. */
  ExpressionNode* right_;
};

/** 
 * An expression tree, used to represent and evaluate mathematical 
 * expressions. It consists of ExpressionNode objects, which form
 * a binary tree. 
 */
class ExpressionTree {
 public:
  // CONSTRUCTORS AND DESTRUCTORS
  /** Empty constructor. */
  ExpressionTree() {}
  /** 
   * Simple constructor that takes as input a tree root, and computes
   * the set of variables the tree involves. 
   */
  ExpressionTree(ExpressionNode* root);
  /** Destructor that deletes all the tree nodes. */
  ~ExpressionTree();
 
  // ACCESSORS
  /** Returns the variables involved in the expression. */
  const std::set<std::string>& vars() const { return vars_; }

  // TREE METHODS
  /** 
   * It evaluates the expression represented in the tree, substituting the 
   * variables with their respective values provided in the input. 
   */
  double evaluate(const std::map<std::string, double>& var_values) const;

 private:
  /** The tree root. */
  ExpressionNode* root_;
  /** The names of the variables included in the tree. */
  std::set<std::string> vars_;
};
