#include "array_schema.h"
#include <iostream>

int main() {
  // Set attribute names
  std::vector<std::string> attribute_names;
  attribute_names.push_back("attr1"); 
  attribute_names.push_back("attr2"); 

  // Set attribute types
  std::vector<ArraySchema::DataType> attribute_types;
  attribute_types.push_back(ArraySchema::INT);
  attribute_types.push_back(ArraySchema::FLOAT);
  
  // Set dimension names
  std::vector<std::string> dim_names;
  dim_names.push_back("i"); 
  dim_names.push_back("j"); 
 
  // Set dimension type 
  ArraySchema::DataType dim_type = ArraySchema::INT64_T;
  
  // Set dimension domains
  std::vector<std::pair<double,double> > dim_domains;
  dim_domains.push_back(std::pair<double,double>(0, 7));
  dim_domains.push_back(std::pair<double,double>(0, 12));

  std::vector<std::pair<double,double> > dim_domains_2;
  dim_domains_2.push_back(std::pair<double,double>(0, 130));
  dim_domains_2.push_back(std::pair<double,double>(0, 110));

  // Set tile extents 
  std::vector<double> tile_extents;
  tile_extents.push_back(30);
  tile_extents.push_back(40);

  // Set coordinates 
  std::vector<int64_t> coordinates;
  coordinates.push_back(3);
  coordinates.push_back(2);
  
  std::vector<int64_t> coordinates_2;
  coordinates_2.push_back(41);
  coordinates_2.push_back(90);
  
  try {
    // Create an array with irregular tiles
    ArraySchema A("A",
                  attribute_names,
                  attribute_types,
                  dim_domains,
                  dim_names,
                  dim_type);

    // Calculate a Hilber cell id
    std::cout << "Hilbert cell id of (3,2) in A: " 
              << A.cell_id_hilbert(coordinates) << "\n";

    // Create an an array with regular tiles
    ArraySchema B("B",
                  attribute_names,
                  attribute_types,
                  dim_domains_2,
                  dim_names,
                  dim_type,
                  tile_extents);

    // Calculate tile ids according to row-major, column-major and 
    // Hilbert order
    std::cout << "Row major tile id of (41,90) in B: " 
              << B.tile_id_row_major(coordinates_2) << "\n";
    std::cout << "Column major tile id of (41,90) in B: " 
              << B.tile_id_column_major(coordinates_2) << "\n";
    std::cout << "Hilbert tile id of (41,90) in B: " 
              << B.tile_id_hilbert(coordinates_2) << "\n";
  }catch(ArraySchemaException& ase) { // Catch exceptions
    std::cout << ase.what() << "\n";
  } 

  return 0;
}
