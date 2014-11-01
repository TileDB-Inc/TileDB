#include "loader.h"
#include "query_processor.h"
#include <iostream>

int main() {

  std::cout << "Testing QueryProcessor...\n";

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
    ArraySchema array_schema_result_ireg("Ri", attribute_names, attribute_types,
                                 dim_domains, dim_names, dim_type);
    ArraySchema array_schema_result_reg("Rr", attribute_names, attribute_types,
                                 dim_domains, dim_names, dim_type,
                                 tile_extents);

    StorageManager sm("~/MIT/TileDB/Data");
    Loader ld("~/MIT/TileDB/Data", sm);
    QueryProcessor qp(sm);

    // sm.delete_array(array_schema_reg.array_name());
    // sm.delete_array(array_schema_ireg.array_name());
    sm.delete_array(array_schema_result_ireg.array_name());
    //sm.delete_array(array_schema_result_reg.array_name());
    //ld.load("~/MIT/TileDB/Data/test.csv", array_schema_reg, Loader::HILBERT);
    // ld.load("~/MIT/TileDB/Data/test.csv", array_schema_ireg, Loader::COLUMN_MAJOR);
    // qp.export_to_CSV(array_schema_reg, "~/MIT/TileDB/Data/B.csv");
    // qp.export_to_CSV(array_schema_ireg, "~/MIT/TileDB/Data/B.csv");
   
   // sm.delete_array(array_schema_ireg.array_name());
 
    std::vector<double> range;
    range.push_back(9);
    range.push_back(11);
    range.push_back(10);
    range.push_back(13);

    qp.subarray(array_schema_ireg, range, array_schema_result_ireg.array_name());
    qp.export_to_CSV(array_schema_result_ireg, "~/MIT/TileDB/Data/Ri.csv");

  } catch(StorageManagerException& sme) {
    std::cout << sme.what() << "\n";
  } catch(LoaderException& le) {
    std::cout << le.what() << "\n";
  } catch(QueryProcessorException& qpe) {
    std::cout << qpe.what() << "\n";
  }

  return 0;
}
