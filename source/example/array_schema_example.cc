#include "array_schema.h"
#include <iostream>

int main() {
  std::vector<std::string> attribute_names;
  attribute_names.push_back("attr1"); 
  attribute_names.push_back("attr2"); 

  std::vector<ArraySchema::DataType> attribute_types;
  attribute_types.push_back(ArraySchema::INT);
  attribute_types.push_back(ArraySchema::FLOAT);
  
  std::vector<std::string> dim_names;
  dim_names.push_back("i"); 
  dim_names.push_back("j"); 
  
  ArraySchema::DataType dim_type = ArraySchema::INT64_T;
  
  std::vector<std::pair<double,double> > dim_domains;
  dim_domains.push_back(std::pair<double,double>(0, 7));
  dim_domains.push_back(std::pair<double,double>(0, 12));

  std::vector<std::pair<double,double> > dim_domains_2;
  dim_domains_2.push_back(std::pair<double,double>(0, 130));
  dim_domains_2.push_back(std::pair<double,double>(0, 110));
 
  std::vector<double> tile_extents;
  tile_extents.push_back(30);
  tile_extents.push_back(40);
 
  std::vector<int64_t> coordinates;
  coordinates.push_back(3);
  coordinates.push_back(2);
  
  std::vector<int64_t> coordinates_2;
  coordinates_2.push_back(41);
  coordinates_2.push_back(90);
  
  try {
    ArraySchema A("A",
                  attribute_names,
                  attribute_types,
                  dim_domains,
                  dim_names,
                  dim_type);


    std::cout << "Hilbert cell id of (3,2): " 
              << A.cell_id_hilbert(coordinates) << "\n";

    ArraySchema B("B",
                  attribute_names,
                  attribute_types,
                  dim_domains_2,
                  dim_names,
                  dim_type,
                  tile_extents);

    std::cout << "Row major tile id of (41,90): " 
              << B.tile_id_row_major(coordinates_2) << "\n";
    std::cout << "Column major tile id of (41,90): " 
              << B.tile_id_column_major(coordinates_2) << "\n";
    std::cout << "Hilbert tile id of (41,90): " 
              << B.tile_id_hilbert(coordinates_2) << "\n";
  }catch(ArraySchemaException& ase) {
    std::cout << ase.what() << "\n";
  } 

  return 0;
}
