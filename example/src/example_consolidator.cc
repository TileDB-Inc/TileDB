/**  
 * @file   example_consolidator.cc
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
 * This file demonstrates the usage of Consolidator objects.
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

int main() {

  // Prepare some array schemas
  ArraySchema array_schema_REG_A = 
      create_array_schema_A(true);    // Regular tiles
  ArraySchema array_schema_IREG_A = 
      create_array_schema_A(false);   // Irregular tiles

  // Create storage manager
  // The input is the path to its workspace (the path must exist).
  StorageManager sm(
      "~/stavrospapadopoulos/TileDB/data/example_consolidator");

  // Create query processor
  // The first input is the path to its workspace (the path must exist).
  QueryProcessor qp(
      "~/stavrospapadopoulos/TileDB/data/example_consolidator", sm);

  // Create loader
  // The first input is the path to its workspace (the path must exist).
  Loader ld("~/stavrospapadopoulos/TileDB/data/example_consolidator", sm);
  
  // Create consolidator
  // The first input is the path to its workspace (the path must exist).
  Consolidator cn("~/stavrospapadopoulos/TileDB/data/example_consolidator", sm);

  //------ //
  // Loads //
  // ----- //
  std::cout << "Loads...\n";
  // Clone schemas for the 0-th (i.e., first) fragment
  ArraySchema fragment_schema_REG_A_0 = 
      array_schema_REG_A.clone(array_schema_REG_A.array_name() + "_0_0");
  ArraySchema fragment_schema_IREG_A_0 = 
      array_schema_IREG_A.clone(array_schema_IREG_A.array_name() + "_0_0");
  // Create the 0-th fragments via loading
  // Loading works as explained in example_query_processor.cc: each fragment
  // is essentially treated as a separate (independent) array
  ld.load("~/stavrospapadopoulos/TileDB/data/test_A_0.csv", 
                fragment_schema_REG_A_0);
  ld.load("~/stavrospapadopoulos/TileDB/data/test_A_0.csv", 
                fragment_schema_IREG_A_0);
  // Update the fragment information of the array at the consolidator
  // This is important in order to be prepare the consolidator for future
  // updates, so that it knows what fragments to consolidate and when 
  const Consolidator::ArrayDescriptor* cn_ad_REG_A = 
      cn.open_array(array_schema_REG_A);
  cn.add_fragment(cn_ad_REG_A);  
  cn.close_array(cn_ad_REG_A);
  const Consolidator::ArrayDescriptor* cn_ad_IREG_A = 
      cn.open_array(array_schema_IREG_A);
  cn.add_fragment(cn_ad_IREG_A);  
  cn.close_array(cn_ad_IREG_A);

  //-------- //
  // Updates //
  // ------- //
  std::cout << "Updates...\n";
  // Clone schemas for the next (i.e., second) fragment
  // Get the next fragment sequence number from the consolidator first
  cn_ad_REG_A = cn.open_array(array_schema_REG_A);
  std::string fragment_name;
  fragment_name = cn.get_next_fragment_name(cn_ad_REG_A);
  ArraySchema fragment_schema_REG_A_1 = 
      array_schema_REG_A.clone(fragment_name);
  cn_ad_IREG_A = cn.open_array(array_schema_IREG_A);
  fragment_name = cn.get_next_fragment_name(cn_ad_IREG_A);
  ArraySchema fragment_schema_IREG_A_1 = 
      array_schema_IREG_A.clone(fragment_name);
  // Create the 1-st fragments via loading
  // Loading works as explained in example_query_processor.cc: each fragment
  // is essentially treated as a separate (independent) array
  ld.load("~/stavrospapadopoulos/TileDB/data/test_A_1.csv", 
                fragment_schema_REG_A_1);
  ld.load("~/stavrospapadopoulos/TileDB/data/test_A_1.csv", 
                fragment_schema_IREG_A_1);
  // Update the fragment information of the array at the consolidator
  // This is important in order to be prepare the consolidator for future
  // updates, so that it knows what fragments to consolidate and when 
  cn.add_fragment(cn_ad_REG_A);  
  cn.close_array(cn_ad_REG_A);
  cn.add_fragment(cn_ad_IREG_A);  
  cn.close_array(cn_ad_IREG_A);
  // Clone schemas for the next (i.e., third) fragment
  // Get the next fragment sequence number from the consolidator first
  cn_ad_REG_A = cn.open_array(array_schema_REG_A);
  fragment_name = cn.get_next_fragment_name(cn_ad_REG_A);
  ArraySchema fragment_schema_REG_A_2 = 
      array_schema_REG_A.clone(fragment_name);
  cn_ad_IREG_A = cn.open_array(array_schema_IREG_A);
  fragment_name = cn.get_next_fragment_name(cn_ad_IREG_A);
  ArraySchema fragment_schema_IREG_A_2 = 
      array_schema_IREG_A.clone(fragment_name);
  // Create the 2-nd fragments via loading
  // Loading works as explained in example_query_processor.cc: each fragment
  // is essentially treated as a separate (independent) array
  ld.load("~/stavrospapadopoulos/TileDB/data/test_A_2.csv", 
                fragment_schema_REG_A_2);
  ld.load("~/stavrospapadopoulos/TileDB/data/test_A_2.csv", 
                fragment_schema_IREG_A_2);
  // Update the fragment information of the array at the consolidator
  // This is important in order to be prepare the consolidator for future
  // updates, so that it knows what fragments to consolidate and when 
  // NOTE: After the following commands, due to the fact that the default
  // consolidation step is 3, the consolidator will merge the 3 segments
  // (of both REG and IREG cases) into a single one.
  cn.add_fragment(cn_ad_REG_A);  
  cn.close_array(cn_ad_REG_A);
  cn.add_fragment(cn_ad_IREG_A);  
  cn.close_array(cn_ad_IREG_A);

  // ------------- //
  // Export to CSV //
  // ------------- //
  std::cout << "Export to CSV...\n";
  // Get fragment suffixes (which indicate which fragments are present
  // for each array) 
  cn_ad_REG_A = cn.open_array(array_schema_REG_A);
  std::vector<std::string> fragment_suffixes_REG_A = 
      cn.get_all_fragment_suffixes(cn_ad_REG_A);  
  cn.close_array(cn_ad_REG_A);
  cn_ad_IREG_A = cn.open_array(array_schema_IREG_A);
  std::vector<std::string> fragment_suffixes_IREG_A = 
      cn.get_all_fragment_suffixes(cn_ad_IREG_A);  
  cn.close_array(cn_ad_IREG_A);
  // Currently we do not support queries on multiple fragments
  // The array must consist of one (potentially consolidated) fragment
  if(fragment_suffixes_REG_A.size() == 1)  {  // Single fragment
    const StorageManager::ArrayDescriptor* ad_REG_A = 
        sm.open_array(array_schema_REG_A.array_name() + 
                     std::string("_") + fragment_suffixes_REG_A[0]);
    qp.export_to_CSV(ad_REG_A, "consolidated_REG_A.csv");
    sm.close_array(ad_REG_A);
  } else {
    std::cout << "Queries on multiple fragments not supported yet!\n";
    exit(0);
  }
  if(fragment_suffixes_IREG_A.size() == 1)  {  // Single fragment
     const StorageManager::ArrayDescriptor* ad_IREG_A = 
        sm.open_array(array_schema_IREG_A.array_name() + 
                     std::string("_") + fragment_suffixes_IREG_A[0]);
     qp.export_to_CSV(ad_IREG_A, "consolidated_IREG_A.csv");
     sm.close_array(ad_IREG_A);
   } else {
     std::cout << "Queries on multiple fragments not supported yet!\n";
     exit(0);
   }

  // ------ //
  // Filter //
  // ------ //
  std::cout << "Filter...\n";
  // Create an expression tree that represents the filter condition
  // (this will typically be created by the parser of the user's command).
  // Expression: attr1 >= 100
  ExpressionNode* n_attr_1 = new ExpressionNode("attr1");
  ExpressionNode* n_100 = new ExpressionNode(100);
  ExpressionNode* n_gteq = 
      new ExpressionNode(ExpressionNode::GTEQ, n_attr_1, n_100);
  ExpressionTree* expression = new ExpressionTree(n_gteq);
  // Get fragment suffixes (which indicate which fragments are present
  // for each array) 
  cn_ad_REG_A = cn.open_array(array_schema_REG_A);
  fragment_suffixes_REG_A.clear();
  fragment_suffixes_REG_A = cn.get_all_fragment_suffixes(cn_ad_REG_A);  
  cn.close_array(cn_ad_REG_A);
  cn_ad_IREG_A = cn.open_array(array_schema_IREG_A);
  fragment_suffixes_IREG_A.clear();
  fragment_suffixes_IREG_A = cn.get_all_fragment_suffixes(cn_ad_IREG_A);  
  cn.close_array(cn_ad_IREG_A);
  // Currently we do not support queries on multiple fragments
  // The array must consist of one (potentially consolidated) fragment
  if(fragment_suffixes_REG_A.size() == 1)  {  // Single fragment
    const StorageManager::ArrayDescriptor* ad_REG_A = 
        sm.open_array(array_schema_REG_A.array_name() + 
                     std::string("_") + fragment_suffixes_REG_A[0]);
    qp.filter(ad_REG_A, expression, "filter_REG_A.csv");
    sm.close_array(ad_REG_A);
  } else {
    std::cout << "Queries on multiple fragments not supported yet!\n";
    exit(0);
  }
  if(fragment_suffixes_IREG_A.size() == 1)  {  // Single fragment
     const StorageManager::ArrayDescriptor* ad_IREG_A = 
        sm.open_array(array_schema_IREG_A.array_name() + 
                     std::string("_") + fragment_suffixes_IREG_A[0]);
     qp.filter(ad_IREG_A, expression, "filter_IREG_A.csv");
     sm.close_array(ad_IREG_A);
  } else {
    std::cout << "Queries on multiple fragments not supported yet!\n";
    exit(0);
  }
  // Update the fragment information of the result arrays at the consolidator.
  ArraySchema filter_array_schema_REG_A = 
      array_schema_REG_A.clone("filter_REG_A_0_0");
  ArraySchema filter_array_schema_IREG_A = 
      array_schema_IREG_A.clone("filter_IREG_A_0_0");
  cn_ad_REG_A = cn.open_array(filter_array_schema_REG_A);
  cn_ad_IREG_A = cn.open_array(filter_array_schema_IREG_A);
  cn.add_fragment(cn_ad_REG_A);  
  cn.add_fragment(cn_ad_IREG_A);  
  cn.close_array(cn_ad_REG_A);
  cn.close_array(cn_ad_IREG_A);

  // Clean up
  delete expression;

  // NOTE #1: The same logic as in filter follows for the other queries as well

  // NOTE #2: The Consolidator is used in a more elegant way inside the 
  // Executor.

  std::cout << "Done!\n";

  return 0;
}

