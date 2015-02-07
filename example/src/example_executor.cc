/**  
 * @file   example_executor.cc
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
 * This file demonstrates the usage of Executor objects.
 */

#include "executor.h"
#include <iostream>
#include <stdlib.h>

// Returns an array schema
ArraySchema create_array_schema_A(bool regular) {
  // Set attribute names
  std::vector<std::string> attribute_names;
  attribute_names.push_back("attr1"); 
  attribute_names.push_back("attr2"); 
 
  // Set dimension names
  std::vector<std::string> dim_names;
  dim_names.push_back("i"); 
  dim_names.push_back("j"); 
 
  // Set dimension domains
  std::vector<std::pair<double,double> > dim_domains;
  dim_domains.push_back(std::pair<double,double>(0, 50));
  dim_domains.push_back(std::pair<double,double>(0, 50));

  // Set tile extents 
  std::vector<double> tile_extents;
  tile_extents.push_back(3);
  tile_extents.push_back(4);

  // Set attribute types. The first two types are for the attributes,
  // and the third type is for all the dimensions collectively. Recall
  // that the dimensions determine the cell coordinates, which are 
  // collectively regarded as an extra attribute.
  std::vector<const std::type_info*> types;
  types.push_back(&typeid(int));
  types.push_back(&typeid(float));
  types.push_back(&typeid(int64_t));

  // Set order and capacity
  ArraySchema::Order order = ArraySchema::HILBERT;
  uint64_t capacity = 5;

  // Return schema with regular tiles
  if(regular)
    return ArraySchema("REG_A", attribute_names, dim_names, dim_domains, 
                       types, order, tile_extents, capacity);
  // Return schema with irregular tiles
  else 
    return ArraySchema("IREG_A", attribute_names, dim_names, dim_domains, 
                       types, order, capacity);
}

// Returns an array schema
ArraySchema create_array_schema_B(bool regular) {
  // Set attribute names
  std::vector<std::string> attribute_names;
  attribute_names.push_back("B_attr_1"); 
  attribute_names.push_back("attr1"); 
  attribute_names.push_back("attr2"); 
 
  // Set dimension names
  std::vector<std::string> dim_names;
  dim_names.push_back("B_i"); 
  dim_names.push_back("B_j"); 
 
  // Set dimension domains
  std::vector<std::pair<double,double> > dim_domains;
  dim_domains.push_back(std::pair<double,double>(0, 50));
  dim_domains.push_back(std::pair<double,double>(0, 50));

  // Set tile extents 
  std::vector<double> tile_extents;
  tile_extents.push_back(3);
  tile_extents.push_back(4);

  // Set attribute types. The first two types are for the attributes,
  // and the third type is for all the dimensions collectively. Recall
  // that the dimensions determine the cell coordinates, which are 
  // collectively regarded as an extra attribute.
  std::vector<const std::type_info*> types;
  types.push_back(&typeid(int64_t));
  types.push_back(&typeid(int));
  types.push_back(&typeid(float));
  types.push_back(&typeid(int64_t));

  // Set order and capacity
  ArraySchema::Order order = ArraySchema::HILBERT;
  uint64_t capacity = 10000;

  // Return schema with regular tiles
  if(regular)
    return ArraySchema("REG_B", attribute_names, dim_names, dim_domains, 
                       types, order, tile_extents, capacity);
  // Return schema with irregular tiles
  else 
    return ArraySchema("IREG_B", attribute_names, dim_names, dim_domains, 
                       types, order, capacity);
}

int main() {

  // Prepare some array schemas
  ArraySchema array_schema_REG_A = 
      create_array_schema_A(true);    // Regular tiles
  ArraySchema array_schema_IREG_A = 
      create_array_schema_A(false);   // Irregular tiles
  ArraySchema array_schema_REG_B = 
      create_array_schema_B(true);    // Regular tiles
  ArraySchema array_schema_IREG_B = 
      create_array_schema_B(false);   // Irregular tiles
 
  // Create an executor
  // The input is the path to its workspace (the path must exist).
  Executor executor(  
      "~/stavrospapadopoulos/TileDB/data/example_executor");

  try {
    //------ //
    // Loads //
    // ----- //
    std::cout << "Loads...\n";
    executor.load("~/stavrospapadopoulos/TileDB/data/test_A_0.csv", 
                  array_schema_REG_A);
    executor.load("~/stavrospapadopoulos/TileDB/data/test_A_0.csv", 
                  array_schema_IREG_A);
//    executor.load("~/stavrospapadopoulos/TileDB/data/test_B.csv", 
//                  array_schema_REG_B);
//    executor.load("~/stavrospapadopoulos/TileDB/data/test_B.csv", 
//                  array_schema_IREG_B);
    //-------- //
    // Updates //
    // ------- //
    std::cout << "Updates...\n";
    executor.update("~/stavrospapadopoulos/TileDB/data/test_A_1.csv", 
                    array_schema_REG_A);
//    executor.update("~/stavrospapadopoulos/TileDB/data/test_A_2.csv", 
//                    array_schema_REG_A);
    executor.update("~/stavrospapadopoulos/TileDB/data/test_A_1.csv", 
                    array_schema_IREG_A);
//    executor.update("~/stavrospapadopoulos/TileDB/data/test_A_2.csv", 
//                    array_schema_IREG_A);

    // ------------- //
    // Export to CSV //
    // ------------- //
    std::cout << "Export to CSV...\n";
    executor.export_to_CSV("consolidated_REG_A.csv", array_schema_REG_A);
    executor.export_to_CSV("consolidated_IREG_A.csv", array_schema_IREG_A);

    // ------ //
    // Filter //
    // ------ //
    std::cout << "Filter...\n";
    // Create an expression tree that represents the filter condition
    // (this will typically be created by the parser of the user's command).
    // Expression: attr1 >= 5
    ExpressionNode* n_attr_1 = new ExpressionNode("attr1");
    ExpressionNode* n_100 = new ExpressionNode(5);
    ExpressionNode* n_gteq = 
        new ExpressionNode(ExpressionNode::GTEQ, n_attr_1, n_100);
    ExpressionTree* expression = new ExpressionTree(n_gteq);
    // Perform the filter
    executor.filter(array_schema_REG_A, expression, "filter_REG_A");  
    executor.filter(array_schema_IREG_A, expression, "filter_IREG_A");  
    // Export result
    std::cout << "Export filter result...\n";
    ArraySchema filter_array_schema_REG_A = 
        array_schema_REG_A.clone("filter_REG_A");
    ArraySchema filter_array_schema_IREG_A = 
        array_schema_IREG_A.clone("filter_IREG_A");
    executor.export_to_CSV("filter_REG_A.csv", filter_array_schema_REG_A);
    executor.export_to_CSV("filter_IREG_A.csv", filter_array_schema_IREG_A);
    // Clean up
    delete expression;

    // -------- //
    // Subarray //
    // -------- //
    std::cout << "Subarray...\n";
    // Prepare a range ([16,19], [20,21])
    Tile::Range range;
    range.push_back(16); range.push_back(19);
    range.push_back(20); range.push_back(21);
    // Perform the subarray
    executor.subarray(array_schema_REG_A, range, "subarray_REG_A");  
    executor.subarray(array_schema_IREG_A, range, "subarray_IREG_A");  
    // Export result
    std::cout << "Export subarray result...\n";
    ArraySchema subarray_array_schema_REG_A = 
        array_schema_REG_A.clone("subarray_REG_A");
    ArraySchema subarray_array_schema_IREG_A = 
        array_schema_REG_A.clone("subarray_IREG_A");
    executor.export_to_CSV("subarray_REG_A.csv", subarray_array_schema_REG_A);
    executor.export_to_CSV("subarray_IREG_A.csv", subarray_array_schema_IREG_A);

/*
    // ----------------- //
    // Nearest neighbors //
    // ----------------- //
    std::cout << "Nearest neighbors...\n";
    // Prepare query point q and number of results k
    std::vector<double> q;
    q.push_back(35); q.push_back(32);
    uint64_t k = 5;
    // Perform the nearest neighbor search
    executor.nearest_neighbors(array_schema_REG_A, q, k, "nn_REG_A");  
    executor.nearest_neighbors(array_schema_IREG_A, q, k, "nn_IREG_A");  
    // Export result
    std::cout << "Export nearest neighbors result...\n";
    ArraySchema nn_array_schema_REG_A = 
        array_schema_REG_A.clone("nn_REG_A");
    ArraySchema nn_array_schema_IREG_A = 
        array_schema_REG_A.clone("nn_IREG_A");
    executor.export_to_CSV("nn_REG_A.csv", nn_array_schema_REG_A);
    executor.export_to_CSV("nn_IREG_A.csv", nn_array_schema_IREG_A);
*/
  
/*
    // ---- //
    // Join //
    // ---- //
    std::cout << "Join...\n";
    executor.join(array_schema_IREG_A, array_schema_IREG_B, "join_IREG_C");
    executor.join(array_schema_REG_A, array_schema_REG_B, "join_REG_C");
    // Export result
    std::cout << "Export join result...\n";
    ArraySchema join_array_schema_IREG_C = 
        ArraySchema::create_join_result_schema(
            array_schema_IREG_A, array_schema_IREG_B, "join_IREG_C");
    ArraySchema join_array_schema_REG_C = 
        ArraySchema::create_join_result_schema(
            array_schema_REG_A, array_schema_REG_B, "join_REG_C");
    executor.export_to_CSV("join_IREG_C.csv", join_array_schema_IREG_C);
    executor.export_to_CSV("join_REG_C.csv", join_array_schema_REG_C);
*/

    // ------------ //
    // Delete array //
    // ------------ //
    std::cout << "Delete array...\n";
    // Create two array fragments for the same array
    ArraySchema del_array_schema_REG_A = 
        array_schema_REG_A.clone("del_REG_A");
    ArraySchema del_array_schema_IREG_A = 
        array_schema_REG_A.clone("del_IREG_A");
    executor.load("~/stavrospapadopoulos/TileDB/data/test_A_0.csv", 
                  del_array_schema_REG_A);
    executor.load("~/stavrospapadopoulos/TileDB/data/test_A_0.csv", 
                  del_array_schema_IREG_A);
    executor.update("~/stavrospapadopoulos/TileDB/data/test_A_1.csv", 
                    del_array_schema_REG_A);
    executor.update("~/stavrospapadopoulos/TileDB/data/test_A_1.csv", 
                    del_array_schema_IREG_A);
    // Delete array
    executor.delete_array(del_array_schema_REG_A);
    executor.delete_array(del_array_schema_IREG_A);

    std::cout << "Done!\n";

  } catch(ExecutorException& ee) {
    std::cout << ee.what() << "\n";
    exit(-1);
  }

  return 0;
}

