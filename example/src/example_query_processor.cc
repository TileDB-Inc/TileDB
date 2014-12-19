#include "loader.h"
#include "query_processor.h"
#include <iostream>


// Returns an array schema
ArraySchema create_array_schema(bool regular) {
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
    return ArraySchema("REG", attribute_names, dim_names, dim_domains, 
                       types, order, tile_extents, capacity);
  // Return schema with irregular tiles
  else 
    return ArraySchema("IREG", attribute_names, dim_names, dim_domains, 
                       types, order, capacity);
}

int main() {
  // Prepare some array schemas
  ArraySchema array_schema_REG = 
      create_array_schema(true);    // Regular tiles
  ArraySchema array_schema_IREG = 
      create_array_schema(false);   // Irregular tiles

  // Prepare a range ([16,19], [20,21])
  Tile::Range range;
  range.push_back(16); range.push_back(19);
  range.push_back(20); range.push_back(21);

  // Create storage manager
  // The input is the path to its workspace (the path must exist).
  StorageManager sm("~/stavrospapadopoulos/TileDB/data");

  // Create query processor
  // The first input is the path to its workspace (the path must exist).
  QueryProcessor qp("~/stavrospapadopoulos/TileDB/data", sm);

  // Open arrays in READ mode
  const StorageManager::ArrayDescriptor* ad_REG = 
      sm.open_array(array_schema_REG.array_name());
  const StorageManager::ArrayDescriptor* ad_IREG = 
      sm.open_array(array_schema_IREG.array_name());

  // ------------- //
  // Export to CSV //
  // ------------- //
  qp.export_to_CSV(ad_REG, "REG_test.csv");
  qp.export_to_CSV(ad_IREG, "IREG_test.csv");

  // -------- //
  // Subarray //
  // -------- //
  qp.subarray(ad_REG, range, "R_REG"); 
  qp.subarray(ad_IREG, range, "R_IREG"); 

  // Export the results to CSV
  const StorageManager::ArrayDescriptor* ad_R_REG = sm.open_array("R_REG");
  const StorageManager::ArrayDescriptor* ad_R_IREG = sm.open_array("R_IREG");
  qp.export_to_CSV(ad_R_REG, "REG_R_test.csv");
  qp.export_to_CSV(ad_R_IREG, "IREG_R_test.csv");
  
  // Close array
  sm.close_array(ad_REG);
  sm.close_array(ad_IREG);
  sm.close_array(ad_R_REG);
  sm.close_array(ad_R_IREG);

  return 0;
}

