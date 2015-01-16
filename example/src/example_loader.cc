/**
 * @file   example_loader.cc
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
 * This file demonstrates the usage of Loader objects. The loader takes as
 * input a CSV file with raw array data, and creates an array in the
 * native (binary) TileDB format based on the array schema.
 */

#include "loader.h"
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
    return ArraySchema("REG_A", attribute_names, dim_names, dim_domains, 
                       types, order, tile_extents, capacity);
  // Return schema with irregular tiles
  else 
    return ArraySchema("IREG_A", attribute_names, dim_names, dim_domains, 
                       types, order, capacity);
}

int main() {

  try {
    // Prepare some array schemas
    ArraySchema array_schema_REG = 
        create_array_schema(true);    // Regular tiles
    ArraySchema array_schema_IREG = 
        create_array_schema(false);   // Irregular tiles

    // Create storage manager
    // The input is the path to its workspace (the path must exist).
    StorageManager sm("~/stavrospapadopoulos/TileDB/data/example_loader");

    // Create loader
    // The first input is the path to its workspace (the path must exist).
    Loader ld("~/stavrospapadopoulos/TileDB/data/example_loader", sm);

    // Delete the arrays if they already exist
    sm.delete_array(array_schema_REG.array_name());
    sm.delete_array(array_schema_IREG.array_name());

    // Load a CSV file
    ld.load("~/stavrospapadopoulos/TileDB/data/test_A.csv", array_schema_REG);
    ld.load("~/stavrospapadopoulos/TileDB/data/test_A.csv", array_schema_IREG);
  } catch(LoaderException& le) {
    std::cout << le.what() << "\n";
  }

  return 0;
}
