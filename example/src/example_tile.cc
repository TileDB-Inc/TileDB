/**
 * @file   example_tile.cc
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
 * This file demonstrates the usage of CoordinateTile and AttributeTile objects
 * through polymorphism enabled by abstract class Tile. In essence, we always
 * store CoordinateTile and AttributeTile object pointers into Tile pointer
 * variables, and use solely the interface of class Tile. This allows us
 * to store multiple CoordinateTile and AttributeTile objects of variable 
 * template types into (homogeneous) Tile* arrays and access them in a unified 
 * way, avoiding tedious casts and considerably enhancing code readability.
 */

#include "tile.h"
#include <iostream>

int main() {
  // ------------- //
  // Tile creation //
  // ------------- //
  // We will create two attribute tiles and a coordinate tile,
  // all of different types. We will store their pointers to
  // Tile pointer variables, and access them in a unified way
  // using the interface of the Tile class.
  std::vector<Tile*> tiles;
  // Create an attribute tile
  tiles.push_back(new AttributeTile<int>(0));
  // Create an attribute tile, reserving memory for 10 cells
  // (note that this does not mean that the tile will have
  // at most 10 cells - it can have an arbitrary number of cells).
  tiles.push_back(new AttributeTile<double>(0, 10));
  // Create a coordinate tile with 2 dimensions, reserving memory
  // for 10 cells as well (this is optional as in the AttributeTile).
  tiles.push_back(new CoordinateTile<int64_t>(0, 2, 10));

  // --------------- //
  // Appending cells //
  // --------------- //
  std::vector<int64_t> coord;
  coord.resize(2);
  // Cell #1
  (*tiles[0]) << 10;
  (*tiles[1]) << 100;
  coord[0] = 1; coord[1] = 2;
  (*tiles[2]) << coord;
  // Cell #2
  (*tiles[0]) << 20;
  (*tiles[1]) << 200;
  coord[0] = 3; coord[1] = 4;
  (*tiles[2]) << coord;
  // Cell #3
  (*tiles[0]) << 30;
  (*tiles[1]) << 300;
  coord[0] = 5; coord[1] = 6;
  (*tiles[2]) << coord;

  // ------------------ //
  // Printing tile info //
  // ------------------ //
  std::cout << "----- Printing tile info ----- \n";
  tiles[0]->print();
  tiles[1]->print();
  tiles[2]->print();

  // -------------------- //
  // Using cell iterators //
  // -------------------- //
  std::cout << "\n----- Using iterators ----- \n";
  Tile::const_iterator it = tiles[0]->begin();
  Tile::const_iterator it_end = tiles[0]->end();
  std::cout << "Contents of tile #1: \n";
  for(; it != it_end; ++it) {
    // Assign it to a variable first upon the variable definition, 
    // in order to resolve the return type
    int v = *it;
    std::cout << "\t" << v << "\n";
    
    // You cannot do the following: COMPILATION ERROR
    // The return type of operator * cannot be resolved
    // std::cout << *it << "\n";
  }

  // ------------------------------------ //
  // Appending cells using cell iterators //
  // ------------------------------------ //
  std::cout << "\n----- Appending cells through iterators ----- \n";
  Tile* new_tile = new AttributeTile<int>(0);
  it = tiles[0]->begin();
  it_end = tiles[0]->end();
  // Copy the first attribute tile to the new tile
  for(; it != it_end; ++it) 
    *new_tile << it;
  new_tile->print();
  delete new_tile;

  // ---------------- //
  // Simple accessors //
  // ---------------- //
  std::cout << "\n----- Simple accessors ----- \n";
  // Bounding coordinates of tile #3
  std::pair<std::vector<double>, std::vector<double> > bounding_coordinates =
      tiles[2]->bounding_coordinates();
  // --- The following will cause the program to exit because of an assertion
  // --- Attribute tiles do not have bounding coordinates 
  // --- std::pair<std::vector<double>, std::vector<double> > bounding_coordinates =
  // ---    tiles[0]->bounding_coordinates();
  std::cout << "Cell num of tile #2: " << tiles[1]->cell_num() << "\n";
  std::cout << "Cell size of tile #3: " << tiles[2]->cell_size() << "\n";
  // Cell type of tile #1
  const std::type_info* cell_type = tiles[0]->cell_type(); 
  std::cout << "Dim num of tile #3: " << tiles[2]->dim_num() << "\n";
  // --- The following will cause the program to exit because of an assertion
  // --- Attribute tiles do not have dimensions
  // --- std::cout << "Dim num of tile #1: " << tiles[0]->dim_num() << "\n";
  // MBR of tile #3
  std::vector<double> mbr = tiles[2]->mbr();
  // --- The following will cause the program to exit because of an assertion
  // --- Attribute tiles do not have MBRs
  // --- std::vector<double> mbr = tiles[1]->mbr();
  std::cout << "Tile id of tile #2: " << tiles[1]->tile_id() << "\n";
  std::cout << "Tile size of tile #2: " << tiles[1]->tile_size() << "\n";
  std::cout << "Tile type of tile #2: " << 
      ((tiles[1]->tile_type() == Tile::COORDINATE) ? "coordinate\n" 
                                                   : "attribute\n");
  // Copy the payload of tile#3 into buffer
  uint64_t buffer_size = tiles[2]->tile_size();
  char* buffer = new char[buffer_size];
  tiles[2]->copy_payload(buffer); 

  // --------------- //
  // Simple mutators //
  // --------------- //
  std::cout << "\n----- Simple mutators ----- \n";
  Tile* tile = new CoordinateTile<int64_t>(1,2);
  tile->set_payload(buffer, buffer_size);
  tile->set_mbr(tiles[2]->mbr());
  tile->print(); // tile should be identical to tiles[2] with a different id
  
  // ---- //
  // Misc //
  // ---- //
  // Check if a cell is inside a range
  Tile::Range range; // ([2,4], [5, 6])
  range.push_back(2); range.push_back(4);
  range.push_back(1); range.push_back(6);
  std::cout << "Cells in range ([2,4], [5, 6]): \n";
  Tile::const_iterator cell_it = tiles[2]->begin();
  Tile::const_iterator cell_it_end = tiles[2]->end();
  for(unsigned int i=0; cell_it != cell_it_end; cell_it++, i++) {
    if(tiles[2]->cell_inside_range(i, range)) {
      std::vector<int64_t> coordinates = *cell_it;
      std::cout <<  "\t(" << coordinates[0] << "," << coordinates[1] << ")\n";
    }
  }

  // Clean up
  delete [] buffer;
  delete tile;
  for(int i=0; i<tiles.size(); i++)
    delete tiles[i];

  return 0;
}
