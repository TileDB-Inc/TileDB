#include "tile.h"

#include <iostream>
#include <inttypes.h>
#include <vector>

void using_attribute_tiles() {
  std::cout << "Testing AttributeTile...\n";

  // You can create attribute tiles with different cell types
  // They input is the tile id
  AttributeTile<int> at1(0);
  AttributeTile<double> at2(0);
  AttributeTile<float> at3(1);

  // You can append cell values to the tiles via operator << 
  at1 << 100;
  at1 << 200;
  at1 << 300 ;
  at2 << 1.1;
  at2 << 1.2 ;

  // Printing the tile info
  at1.print();
  at2.print();
  
  // You can access cells via the cell() function
  std::cout << "First and second cell: " << at1.cell(0) << " " << at1.cell(1) 
            << "\n";
  // You can access cells via the a payload reference
  std::cout << "First and second cell again: " << at1.payload()[0] << " " 
            << at1.payload()[1] << "\n";
 
  // You can set the payload in a tile  
  std::vector<float> v(2,3.0);
  at3.set_payload(v);
  at3.print();

  // Testing iterators
  std::vector<int>::iterator it = at1.begin();
  std::cout << "Changing the first cell of attribute tile at1 to 400.\n";
  *it = 400;
  std::cout << "Iterating over the payload of attribute tile 1: ";
  for(; it != at1.end(); it++)
    std::cout << *it << " ";
  std::cout << "\n";

  const AttributeTile<int> at4(10);
  // The following is not allowed - COMPILATION ERROR
  // std::vector<int>::iterator it2 = at4.begin();
  // Correct usage (use a const iterator for const objects)
  std::vector<int>::const_iterator it2 = at4.begin();
 
  // Capturing exceptions
  try {
    // The following will throw an excetion because 10 is out of bounds
    std::cout << at1.cell(10);
  } catch(TileException &te) {
    std::cout << "Exception caught in tile: " << te.where() << "\n"
              << te.what() << "\n";
  }
 
  std::cout << "Testing AttributeTile finished.\n\n";
}

void using_coordinate_tiles() {
  std::cout << "Testing CoordinateTile...\n";
  // Testing CoordinateTile
  // The inputs are the tile id and the number of dimensions, respectively
  CoordinateTile<int> ct1(0, 2);
  CoordinateTile<float> ct2(1, 3);

  // You can append cells/coordinates as follows
  std::vector<int> coord_1, coord_2, coord_3;
  coord_1.push_back(3); coord_1.push_back(4);
  coord_2.push_back(1); coord_2.push_back(2);
  coord_3.push_back(5); coord_3.push_back(6);
  ct1 << coord_1;
  ct1 << coord_2;
  ct1 << coord_3;
  ct1.print();

  // You can get coordinates via the cell() function
  std::cout << "First coordinates: " << ct1.cell(0)[0] << " " << ct1.cell(0)[1] 
            << "\n";
  std::cout << "First coordinates again: " << ct1.payload()[0][0] << " " 
            << ct1.payload()[0][1] << "\n";

  // Other accessors
  std::cout << "Dim num: " << ct1.dim_num() << "\n";
  std::cout << "MBR: [" << ct1.mbr()[0] << "," << ct1.mbr()[1] << "], ["
            << ct1.mbr()[2] << "," << ct1.mbr()[3] << "]\n";

  // Some mutators 
  std::vector<float> coord_4(3,3.0);
  std::vector<std::vector<float> > p;
  p.push_back(coord_4);
  ct2.set_payload(p);
  std::vector<double> mbr(6,3.0);
  ct2.set_mbr(mbr);
  ct2.print();

  // Testing exceptions 
  try {
    // The number of dimensions must be larger than 0
    CoordinateTile<int> ct3(1000,0);
  } catch(TileException &te) {
    std::cout << "Exception caught in tile: " << te.where() << "\n"
              << te.what() << "\n";
  }

  std::cout << "Testing CoordinateTile finished.\n\n";
}

void using_abstract_tiles() {
  std::cout << "Testing class Tile...\n";

  AttributeTile<int> at1(0);
  AttributeTile<double> at2(0);
  CoordinateTile<int> ct1(0, 2);
  std::vector<int> coord_1;
  at1 << 100;
  at2 << 200;
  coord_1.push_back(3); coord_1.push_back(4);
  ct1 << coord_1;

  // We can use a Tile object to enforce polymorphism and, thus,  group 
  // attribute/coordinate tiles of differen types in a single array.
  std::vector<Tile*> tiles;
  tiles.push_back(&at1);
  tiles.push_back(&at2);
  tiles.push_back(&ct1);
 
  tiles[0]->print();
  tiles[1]->print();
  tiles[2]->print();

  // We can use << to append cells directly to abstract tiles 
  (*tiles[0]) << 10;
  tiles[0]->print();

  // We can create cell iterators directly on abstract tiles
  Tile::const_iterator it = tiles[0]->begin();
  Tile::const_iterator it_end = tiles[0]->end();
  for(; it != it_end; ++it) {
    // Assign it to the proper type first, so that it internally
    // performs the proper type casting
    int v = *it;
    std::cout << v << "\n";
    
    // You cannot do the following: COMPILATION ERROR
    // It is not respnsible to resolve the return type of operator *
    // std::cout << *it << "\n";
  }
 
  std::cout << "Testing class Tile finished.\n\n";
}

int main() {
  using_attribute_tiles();
  using_coordinate_tiles();
  using_abstract_tiles();
  
  return 0;
}
