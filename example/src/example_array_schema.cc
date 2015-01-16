/**
 * @file   example_array_schema.cc
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
 * This file demonstrates the usage of ArraySchema objects.
 */

#include "array_schema.h"
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
  dim_domains.push_back(std::pair<double,double>(0, 7));
  dim_domains.push_back(std::pair<double,double>(0, 12));

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
  dim_domains.push_back(std::pair<double,double>(0, 7));
  dim_domains.push_back(std::pair<double,double>(0, 12));

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
  // ---------------------------- //
  // Creating ArraySchema objects //
  // ---------------------------- //
  ArraySchema array_schema_REG_A = 
      create_array_schema_A(true);    // Regular tiles
  ArraySchema array_schema_IREG_A = 
      create_array_schema_A(false);   // Irregular tiles
  ArraySchema array_schema_REG_B = 
      create_array_schema_B(true);    // Regular tiles
  ArraySchema array_schema_IREG_B = 
      create_array_schema_B(false);   // Irregular tiles

  // Printing the details of an ArraySchema object
  std::cout << "\n";
  array_schema_REG_A.print();
  std::cout << "\n";
  array_schema_IREG_A.print();
  std::cout << "\n";
  array_schema_REG_B.print();
  std::cout << "\n";
  array_schema_IREG_B.print();
  std::cout << "\n";
  
  // ------- //
  // Cloning //
  // ------- //
  // array_schema_REG_C will be identical to array_schema_REG_B, with the 
  // difference that the array name will be "C" instead of "B".
  ArraySchema array_schema_REG_C = array_schema_REG_B.clone("REG_C");
  array_schema_REG_C.print();
  std::cout << "\n";

  // --------------------------------------------- //
  // Serializing/Deserializing ArraySchema objects //
  // --------------------------------------------- //
  // Serialize array_schema_REG_A into a buffer
  std::pair<char*, uint64_t> serialized_A = array_schema_REG_A.serialize();
  // Deserialize array_schema_REG_A into array_schema_REG_C
  array_schema_REG_C.deserialize(serialized_A.first, serialized_A.second);
  // Now array_schema_REG_C is equivalent to array_schema_REG_A
  array_schema_REG_C.print();

  // ------------------------ //
  // Retrieving cell/tile ids //
  // ------------------------ //
  // Set some coordinates 
  std::vector<int64_t> coordinates;
  coordinates.push_back(3);
  coordinates.push_back(2);
  
  std::cout << "\n";
  // Calculate a Hilber cell id
  std::cout << "Hilbert cell id of (3,2) in IREG_A: " 
            << array_schema_IREG_A.cell_id_hilbert(coordinates) << "\n";
  // Calculate tile ids according to row-major, column-major and 
  // Hilbert order
  std::cout << "Row major tile id of (3,2) in REG_A: " 
            << array_schema_REG_A.tile_id_row_major(coordinates) << "\n";
  std::cout << "Column major tile id of (3,2) in REG_A: " 
            << array_schema_REG_A.tile_id_column_major(coordinates) << "\n";
  std::cout << "Hilbert tile id of (3,2) in REG_A: " 
            << array_schema_REG_A.tile_id_hilbert(coordinates) << "\n";
  std::cout << "\n";

  // ------------------------------------ //
  // Creating the schema of a join result //
  // ------------------------------------ //
  std::pair<bool,std::string> can_join = 
      ArraySchema::join_compatible(array_schema_REG_A, array_schema_REG_B);

  if(!can_join.first)
    std::cout << "Not join-compatible: " << can_join.second << "\n";
  else {
    array_schema_REG_C = ArraySchema::create_join_result_schema(
                             array_schema_REG_A, 
                             array_schema_REG_B, 
                             "REG_C");
    array_schema_REG_C.print();
  }

  return 0;
}
