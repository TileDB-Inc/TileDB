#include "loader.h"
#include "query_processor.h"
#include <iostream>


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
  uint64_t capacity = 1000;

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

  // Prepare a range ([16,19], [20,21])
  Tile::Range range;
  range.push_back(16); range.push_back(19);
  range.push_back(20); range.push_back(21);

  // Create storage manager
  // The input is the path to its workspace (the path must exist).
  StorageManager sm(
      "~/stavrospapadopoulos/TileDB/data/example_query_processor");

  // Create query processor
  // The first input is the path to its workspace (the path must exist).
  QueryProcessor qp(
      "~/stavrospapadopoulos/TileDB/data/example_query_processor", sm);
  
  // Delete arrays if they already exist
  sm.delete_array(array_schema_REG_A.array_name());
  sm.delete_array(array_schema_IREG_A.array_name());
  sm.delete_array(array_schema_REG_B.array_name());
  sm.delete_array(array_schema_IREG_B.array_name());
  
  // Load arrays 
  Loader ld("~/stavrospapadopoulos/TileDB/data/example_query_processor", sm);
  ld.load("~/stavrospapadopoulos/TileDB/data/test_A.csv", array_schema_REG_A);
  ld.load("~/stavrospapadopoulos/TileDB/data/test_A.csv", array_schema_IREG_A);
  ld.load("~/stavrospapadopoulos/TileDB/data/test_B.csv", array_schema_REG_B);
  ld.load("~/stavrospapadopoulos/TileDB/data/test_B.csv", array_schema_IREG_B);

  // Open arrays in READ mode
  const StorageManager::ArrayDescriptor* ad_REG_A = 
      sm.open_array(array_schema_REG_A.array_name());
  const StorageManager::ArrayDescriptor* ad_IREG_A = 
      sm.open_array(array_schema_IREG_A.array_name());
  const StorageManager::ArrayDescriptor* ad_REG_B = 
      sm.open_array(array_schema_REG_B.array_name());
  const StorageManager::ArrayDescriptor* ad_IREG_B = 
      sm.open_array(array_schema_IREG_B.array_name());

  // ------------- //
  // Export to CSV //
  // ------------- //
  qp.export_to_CSV(ad_REG_A, "REG_A_test.csv");
  qp.export_to_CSV(ad_IREG_A, "IREG_A_test.csv");

  // -------- //
  // Subarray //
  // -------- //
  qp.subarray(ad_REG_A, range, "R_REG_A"); 
  qp.subarray(ad_IREG_A, range, "R_IREG_A"); 
  // Export the results to CSV
  const StorageManager::ArrayDescriptor* ad_R_REG_A = sm.open_array("R_REG_A");
  const StorageManager::ArrayDescriptor* ad_R_IREG_A = sm.open_array("R_IREG_A");
  qp.export_to_CSV(ad_R_REG_A, "R_REG_A_test.csv");
  qp.export_to_CSV(ad_R_IREG_A, "R_IREG_A_test.csv");
  
  // ---- //
  // Join //
  // ---- //
  qp.join(ad_IREG_A, ad_IREG_B, "R_IREG_C");
  qp.join(ad_REG_A, ad_REG_B, "R_REG_C");

  // ------ //
  // Filter //
  // ------ //
  // Create an expression tree that represents the filter condition
  // (this will typically be created by the parser of the user's command).
  // Expression: attr_1 >= 100
  ExpressionNode* n_attr_1 = new ExpressionNode("attr_1");
  ExpressionNode* n_100 = new ExpressionNode(100);
  ExpressionNode* n_gteq = 
      new ExpressionNode(ExpressionNode::GTEQ, n_attr_1, n_100);
  ExpressionTree* expression = new ExpressionTree(n_gteq);
  // Perform the filter
  qp.filter(ad_IREG_A, expression, "filter_R_IREG_A");  
  qp.filter(ad_REG_A, expression, "filter_R_REG_A");  

  // ---------------------- //
  // Nearest neighbors (NN) //
  // ---------------------- //
  // Prepare query: q is a point, and k is the number of nearest neighbors
  std::vector<double> q;
  q.push_back(15); q.push_back(16);
  uint64_t k = 3;
  // Perform the NN search
  qp.nearest_neighbors(ad_IREG_A, q, k, "NN_R_IREG_A");  
  qp.nearest_neighbors(ad_REG_A, q, k, "NN_R_REG_A");  

  // Close arrays
  sm.close_array(ad_REG_A);
  sm.close_array(ad_IREG_A);
  sm.close_array(ad_REG_B);
  sm.close_array(ad_IREG_B);
  sm.close_array(ad_R_REG_A);
  sm.close_array(ad_R_IREG_A);

  // Clean up
  delete expression;

  return 0;
}

