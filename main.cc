#include "storage_manager.h"
#include "query_processor.h"
#include "loader.h"
#include <iostream>

const ArraySchema* get_array_schema() {
  // Array name
  std::string array_name = "A";

  // Attribute names
  std::vector<std::string> attribute_names;
  attribute_names.push_back("attr1");
  attribute_names.push_back("attr2");

  // Dimension names
  std::vector<std::string> dim_names;
  dim_names.push_back("i");
  dim_names.push_back("j");

  // Dimension domains
  std::vector<std::pair<double, double> > dim_domains;
  dim_domains.push_back(std::pair<double, double>(0, 100));
  dim_domains.push_back(std::pair<double, double>(0, 100));

  // Types
  std::vector<const std::type_info*> types;
  types.push_back(&typeid(int)); 
  types.push_back(&typeid(double)); 
  types.push_back(&typeid(int64_t)); 

  // Cell order
  ArraySchema::CellOrder cell_order = ArraySchema::CO_HILBERT;

  // Consolidation step
  int consolidation_step = 2;

  return new ArraySchema(array_name, attribute_names, dim_names, dim_domains,
                         types, cell_order, consolidation_step); 
}

int main() {
  // Create a storage manager
  StorageManager storage_manager("~/stavrospapadopoulos/TileDB/example/");

  // Create a loader
  Loader loader(&storage_manager);

  // Create a query processor
  QueryProcessor query_processor(&storage_manager);

  // Define an array
  const ArraySchema* array_schema = get_array_schema();
  storage_manager.define_array(array_schema);

  // Load a CSV file into an array
  std::cout << "Loading CSV file to array A...\n";
  loader.load_csv("~/stavrospapadopoulos/TileDB/data/test_A_0.csv", "A");
  std::cout << "Exporting array A to CSV file...\n";
  query_processor.export_to_csv("A", "export_A.csv");

  // Update A with a new CSV file
  std::cout << "Updating A with a CSV file...\n";
  loader.update_csv("~/stavrospapadopoulos/TileDB/data/test_A_1.csv", "A");
  std::cout << "Exporting again array A to CSV file...\n";
  query_processor.export_to_csv("A", "export_A_upd_1.csv");

  // Update A with a second CSV file
  std::cout << "Updating A with a CSV file...\n";
  loader.update_csv("~/stavrospapadopoulos/TileDB/data/test_A_2.csv", "A");
  std::cout << "Exporting again array A to CSV file...\n";
  query_processor.export_to_csv("A", "export_A_upd_2.csv");

  // Update A with a third CSV file
  std::cout << "Updating A with a CSV file...\n";
  loader.update_csv("~/stavrospapadopoulos/TileDB/data/test_A_3.csv", "A");
  std::cout << "Exporting again array A to CSV file...\n";
  query_processor.export_to_csv("A", "export_A_upd_3.csv");

  // Compute subarray
  int64_t range[4];
  range[0] = 5;  range[1] = 20; 
  range[2] = 15; range[3] = 30;
  std::cout << "Processing subarray query on array A...\n";
  query_processor.subarray("A", range, "sub_A");
  std::cout << "Exporting array sub_A to CSV file...\n";
  query_processor.export_to_csv("sub_A", "export_sub_A.csv");

  // Clean up
  delete array_schema;

  return 0;
}
