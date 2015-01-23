/**
 * @file   example_expression_tree.cc
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
 * This file demonstrates the usage of ExpressionTree.
 */

#include "expression_tree.h"
#include <iostream>

// Expression tree for (a + 5) * (c / d):
//      *
//   +      /
// a   5  c   d
ExpressionNode* create_expression_tree_A() {
  ExpressionNode* n_a = new ExpressionNode("a");
  ExpressionNode* n_5 = new ExpressionNode(5);
  ExpressionNode* n_c = new ExpressionNode("c");
  ExpressionNode* n_d = new ExpressionNode("d");
  ExpressionNode* n_add = new ExpressionNode(ExpressionNode::ADD, n_a, n_5);
  ExpressionNode* n_div = new ExpressionNode(ExpressionNode::DIV, n_c, n_d);
  ExpressionNode* n_mul = new ExpressionNode(ExpressionNode::MUL, n_add, n_div);

  return n_mul;
}

// Expression tree for (a + 5) * b >= 100
//             >=
//      *            100
//   +      b
// a   5  
ExpressionNode* create_expression_tree_B() {
  ExpressionNode* n_a = new ExpressionNode("a");
  ExpressionNode* n_5 = new ExpressionNode(5);
  ExpressionNode* n_b = new ExpressionNode("b");
  ExpressionNode* n_100 = new ExpressionNode(100);
  ExpressionNode* n_add = new ExpressionNode(ExpressionNode::ADD, n_a, n_5);
  ExpressionNode* n_mul = new ExpressionNode(ExpressionNode::MUL, n_add, n_b);
  ExpressionNode* n_gteq = new ExpressionNode(ExpressionNode::GTEQ, n_mul, n_100);

  return n_gteq;
}

int main() {
  // Create some expression trees
  // -- 
  ExpressionNode* root_A = create_expression_tree_A();
  ExpressionTree* tree_A = new ExpressionTree(root_A);
  // --
  ExpressionNode* root_B = create_expression_tree_B();
  ExpressionTree* tree_B = new ExpressionTree(root_B);

  // Assign values to the variables of expression A
  // a = 4
  // c = 5
  // d = 2
  std::map<std::string, double> var_values_A;
  var_values_A["a"] = 4; 
  var_values_A["c"] = 5; 
  var_values_A["d"] = 2; 
  // Assign values to the variables of expression B
  // a = 5
  // b = 11
  std::map<std::string, double> var_values_B;
  var_values_B["a"] = 5; 
  var_values_B["b"] = 11; 

  // Evaluate expressions
  std::cout << "Let a = 4, c = 5, d = 2. Then \n";
  std::cout << "\t(a + 5) * (c / d) = " << root_A->evaluate(var_values_A) << "\n\n";
  std::cout << "Let a = 5, b = 11. Then \n";
  std::cout << "\t((a + 5) * b >= 100) = " << root_B->evaluate(var_values_B) << "\n";
 
  // Delete trees
  delete tree_A; 
  delete tree_B; 
   
  return 0;
}
