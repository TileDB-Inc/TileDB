#include "tile.h"

#include <iostream>
#include <inttypes.h>
#include <vector>

int main() {
  // Testing AttributeTile
  AttributeTile<int> at1(0);
  AttributeTile<double> at2(0);
  AttributeTile<float> at3(1);

  std::cout << "Testing AttributeTile...\n";
  std::cout << "Testing append cell and print...\n";
  at1 << 100;
  at1 << 200;
  at1 << 300 ;
  at1.print();
  at2 << 1.1;
  at2 << 1.2 ;
  at2.print();
  std::cout << "Testing append cell and print finished.\n\n";

  std::cout << "Testing accessors...\n";
  std::cout << "First and second cell: " << at1.cell(0) << " " << at2.cell(1) 
            << "\n";
  std::cout << "First and second cell again: " << at1.payload()[0] << " " 
            << at2.payload()[1] << "\n";
  std::cout << "Testing accessors finished.\n\n";
  
  std::cout << "Testing other mutators...\n";
  std::vector<float> v(2,3.0);
  at3.set_payload(v);
  at3.print();
  std::cout << "Testing other mutators finished.\n";
  
  std::cout << "Testing AttributeTile finished.\n\n";

  // Testing CoordinateTile
  CoordinateTile<int> ct1(0, 2);
  CoordinateTile<float> ct2(1, 3);

  std::cout << "Testing CoordinateTile...\n";
  std::cout << "Testing append cell and print...\n";
  std::vector<int> coord_1, coord_2, coord_3;
  coord_1.push_back(3); coord_1.push_back(4);
  coord_2.push_back(1); coord_2.push_back(2);
  coord_3.push_back(5); coord_3.push_back(6);
  ct1 << coord_1;
  ct1 << coord_2;
  ct1 << coord_3;
  ct1.print();
  std::cout << "Testing append cell and print finished.\n\n";

  std::cout << "Testing accessors...\n";
  std::cout << "First coordinates: " << ct1.cell(0)[0] << " " << ct1.cell(0)[1] 
            << "\n";
  std::cout << "First coordinates again: " << ct1.payload()[0][0] << " " 
            << ct1.payload()[0][1] << "\n";
  std::cout << "Dim num: " << ct1.dim_num() << "\n";
  std::cout << "MBR: [" << ct1.mbr()[0] << "," << ct1.mbr()[1] << "], ["
            << ct1.mbr()[2] << "," << ct1.mbr()[3] << "]\n";
  std::cout << "Testing accessors finished.\n\n";
 
  std::cout << "Testing other mutators...\n";
  std::vector<float> coord_4(3,3.0);
  std::vector<std::vector<float> > p;
  p.push_back(coord_4);
  ct2.set_payload(p);
  std::vector<double> mbr(6,3.0);
  ct2.set_mbr(mbr);
  ct2.print();
  std::cout << "Testing other mutators finished.\n";
  
  std::cout << "Testing CoordinateTile finished.\n\n";

  // Testing iterators
  std::cout << "Testing iterators...\n";
  std::vector<int>::iterator it = at1.begin();
  std::cout << "Changing the first cell of attribute tile 1 to 400.\n";
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
  std::cout << "Testing iterators finished.\n\n";

  // Testing exceptions
  std::cout << "Testing exceptions...\n";
  
  try {
    std::cout << at1.cell(10);
  } catch(TileException &te) {
    std::cout << "Exception caught in tile: " << te.where() << "\n"
              << te.what() << "\n";
  }

  try {
    CoordinateTile<int> ct3(1000,0);
  } catch(TileException &te) {
    std::cout << "Exception caught in tile: " << te.where() << "\n"
              << te.what() << "\n";
  }
  
  std::cout << "Testing exceptions finished.\n\n";

  // Using a Tile object to group attribute/coordinate tiles of different
  // types in a single array.
  std::cout << "Testing polymorphism with class Tile...\n";
  std::vector<Tile*> tiles;
  tiles.push_back(&at1);
  tiles.push_back(&at2);
  tiles.push_back(&ct1);
 
  tiles[0]->print();
  tiles[1]->print();
  tiles[2]->print();
 
  (*tiles[0]) << 10;
  tiles[0]->print();
 
  std::cout << "Testing polymorphism finished.\n\n";

  return 0;
}
