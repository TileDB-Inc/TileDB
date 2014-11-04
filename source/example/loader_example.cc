#include "loader.h"
#include <iostream>

int main() {

  std::cout << "Testing Loader...\n";

  try {
    // Prepare some array schemas
    std::vector<std::string> attribute_names;
    attribute_names.push_back("attr1");
    attribute_names.push_back("attr2");

    std::vector<ArraySchema::DataType> attribute_types;
    attribute_types.push_back(ArraySchema::INT);
    attribute_types.push_back(ArraySchema::INT);

    std::vector<std::pair<double, double> > dim_domains;
    dim_domains.push_back(std::pair<double, double>(0,999));
    dim_domains.push_back(std::pair<double, double>(0,999));

    std::vector<std::string> dim_names;
    dim_names.push_back("dim1");
    dim_names.push_back("dim2");

    ArraySchema::DataType dim_type = ArraySchema::INT;

    std::vector<double> tile_extents;
    tile_extents.push_back(10);
    tile_extents.push_back(10);
    
    ArraySchema array_schema_reg("A", attribute_names, attribute_types,
                                 dim_domains, dim_names, dim_type,
                                 tile_extents);
    ArraySchema array_schema_ireg("B", attribute_names, attribute_types,
                                 dim_domains, dim_names, dim_type);

    // Create storage manager
    // It takes as input a path to its workspace
    StorageManager sm("~/stavrospapadopoulos/TileDB/Data");

    // Create loader
    // It takes as input a path to its workspace
    Loader ld("~/stavrospapadopoulos/TileDB/Data", sm);

    // Load
    // The last argument is the cell order for regular tiles, or the tile order
    // for irregular tiles. It could be HILBERT, ROW_MAJOR, or COLUMN_MAJOR
    ld.load("~/stavrospapadopoulos/TileDB/Data/test.csv", 
            array_schema_reg, Loader::HILBERT);
    ld.load("~/stavrospapadopoulos/TileDB/Data/test.csv", 
            array_schema_ireg, Loader::ROW_MAJOR);
  } catch(StorageManagerException& sme) {
    std::cout << sme.what() << "\n";
  } catch(LoaderException& le) {
    std::cout << le.what() << "\n";
  }

  return 0;
}
