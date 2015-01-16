/**
 * @file   example_storage_manager.cc
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
 * This file demonstrates the usage of StorageManager objects.
 */

#include "storage_manager.h"
#include <iostream>

// Returns an array schema
ArraySchema create_array_schema() {
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

  // Return an array_schema initialized with the info above
  return ArraySchema("A", attribute_names, dim_names, dim_domains, 
                     types, order, capacity);
}

int main() {
  // --------------- //
  // Creating arrays //
  // --------------- //
  // Create a storage manager. The input is the path where its workspace
  // will be created. Note: the path must exist.
  StorageManager sm(
      "~/stavrospapadopoulos/TileDB/data/example_storage_manager");
  // Create an array schema
  ArraySchema array_schema = create_array_schema();

  // Delete the array if it exists
  sm.delete_array(array_schema.array_name());

  // Open a array in CREATE mode
  // Pass the array schema as argument to open an array in READ mode
  StorageManager::ArrayDescriptor* ad;
  ad = sm.open_array(array_schema);

  // Create some tiles
  std::vector<Tile*> tiles;
  tiles.push_back(new AttributeTile<int>(0));
  tiles.push_back(new AttributeTile<float>(0));
  tiles.push_back(new CoordinateTile<int64_t>(0, 2));
  std::vector<int64_t> coord;
  (*tiles[0]) << 10;
  (*tiles[1]) << 100;
  coord.resize(2);
  coord[0] = 1; coord[1] = 2;
  (*tiles[2]) << coord;
  (*tiles[0]) << 20;
  (*tiles[1]) << 200;
  coord[0] = 3; coord[1] = 4;
  (*tiles[2]) << coord;
  (*tiles[0]) << 30;
  (*tiles[1]) << 300;
  coord[0] = 5; coord[1] = 6;
  (*tiles[2]) << coord;
  tiles[0]->print();
  tiles[1]->print();
  tiles[2]->print();

  // Store tiles
  for(int i=0; i<tiles.size(); i++)
    sm.append_tile(tiles[i], ad, i);

  // ALWAYS close the array after it is created.
  sm.close_array(ad);

  // -------------- //
  // Reading arrays //
  // -------------- //
  // For easy reference
  unsigned int attribute_num = array_schema.attribute_num();
  // Open an array in READ mode
  // Pass only the array name as argument to open an array in READ mode
  ad = sm.open_array(array_schema.array_name());
  // Create a tile iterator for each attribute 
  // (do not forget the extra coordinate attribute)
  StorageManager::const_iterator* tile_its = 
      new StorageManager::const_iterator[attribute_num+1];
  for(unsigned int i=0; i<=attribute_num; i++) 
    tile_its[i] = sm.begin(ad, i);
  // We use a single end iterator, since all attributes have the same number
  // of tiles
  StorageManager::const_iterator tile_it_end = sm.end(ad, 0);
  while(tile_its[0] != tile_it_end) {
    for(unsigned int i=0; i<=attribute_num; i++) { 
      // Print the tile
      (*tile_its[i]).print();
      // Increment the iterator
      ++tile_its[i];
    }
  }

  delete [] tile_its;

  return 0;
}
