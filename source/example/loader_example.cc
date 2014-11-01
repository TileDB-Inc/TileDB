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
    StorageManager sm("~/MIT/TileDB/Data");

    // Create loader
    Loader ld("~/MIT/TileDB/Data", sm);

    // Load
    ld.load("~/MIT/TileDB/Data/Loader/test.csv", array_schema_reg, Loader::HILBERT);
    // ld.load("~/MIT/TileDB/Data/Loader/test.csv", array_schema_reg, Loader::ROW_MAJOR);
    // ld.load("~/MIT/TileDB/Data/Loader/test.csv", array_schema_reg, Loader::COLUMN_MAJOR);
    // ld.load("~/MIT/TileDB/Data/Loader/test.csv", array_schema_ireg, Loader::HILBERT);
    // ld.load("~/MIT/TileDB/Data/Loader/test.csv", array_schema_ireg, Loader::ROW_MAJOR);
    // ld.load("~/MIT/TileDB/Data/Loader/test.csv", array_schema_ireg, Loader::COLUMN_MAJOR);
    
  } catch(StorageManagerException& sme) {
    std::cout << sme.what() << "\n";
  } catch(LoaderException& le) {
    std::cout << le.what() << "\n";
  }

  return 0;
}
