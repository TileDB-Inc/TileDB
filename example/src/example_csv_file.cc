/**
 * @file   example_csv_file.cc
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
 * This file demonstrates the usage of CSVLine and CSVFile objects.
 */

#include "csv_file.h"
#include "tile.h"
#include <iostream>

int main() {
  // ------------- //
  // CSVLine usage //
  // ------------- //
  CSVLine line_1, line_2, line_3, line_4, line_5, line_6, line;
  // We can assign a string (or const char*) to a CSVLine object 
  line_1 = "10,5.1,stavros";

  // We can treat a CSVLine object as an output stream
  // Several data types are supported.
  line_2 << 5;
  line_2 << 3.7;
  line_2 << "papadopoulos";
  // Lines starting with '#' are comment lines
  line_3 << "# this is a comment line";
  // We can assign a string and then keep appending data
  line_4 = "11,4.3";
  line_4 << "TileDB";
  // Even vectors are supported
  std::vector<int> vec;
  vec.push_back(1); vec.push_back(2); vec.push_back(3);
  line_5 = vec;

  // Let's print the lines, using the str() function
  std::cout << "Printing CSV lines using str():\n";  
  std::cout << "Line #1:\n\t" << line_1.str() << "\n";
  std::cout << "Line #2:\n\t" << line_2.str() << "\n";
  std::cout << "Line #3:\n\t" << line_3.str() << "\n";
  std::cout << "Line #4:\n\t" << line_4.str() << "\n";
  std::cout << "Line #5:\n\t" << line_5.str() << "\n";
  std::cout << "\n";

  // Printing using operator >> and retrieving strings
  std::cout << "Printing CSV lines using operator >> " 
               "and retrieving strings:\n";  
  std::string s;
  int i=1;
  std::cout << "Line#1:\n";
  while(line_1 >> s) 
    std::cout << "\tvalue#" << i++ << ": " << s << "\n";
  i=1;
  std::cout << "Line#2:\n";
  while(line_2 >> s) 
    std::cout << "\tvalue#" << i++ << ": " << s << "\n";
  i=1;
  std::cout << "Line#3:\n";
  while(line_3 >> s) 
    std::cout << "\tvalue#" << i++ << ": " << s << "\n";
  std::cout << "\n";
  // Printing using operator >> and retrieving various data types
  std::cout << "Printing CSV lines using operator >> " 
               "and retrieving various data types:\n";  
  int v_int;
  float v_float;
  line_4 >> v_int;
  line_4 >> v_float;
  line_4 >> s;
  std::cout << "Line#4:\n";
  std::cout << "\tvalue#1: " << ": " << v_int << "\n";
  std::cout << "\tvalue#2: " << ": " << v_float << "\n";
  std::cout << "\tvalue#3: " << ": " << s << "\n";
  line_5 >> v_int;
  line_5 >> v_float;
  line_5 >> s;
  std::cout << "Line#5:\n";
  std::cout << "\tvalue#1: " << v_int << "\n";
  std::cout << "\tvalue#2: " << v_float << "\n";
  std::cout << "\tvalue#3: " << s << "\n";
  std::cout << "\n";

  // More uses of CSV line
  std::cout << "More uses of CSV line:\n\n";
  Tile* tile = new AttributeTile<int>(0);
  line_6 = "1,2,3,4";
  // Feed CSV values to a tile 
  line_6 >> *tile; // Append value 1 to tile
  line_6 >> *tile; // Append value 2 to tile
  // The reverse works equivalently
  *tile << line_6; // Append value 3 to tile
  *tile << line_6; // Append value 4 to tile
   
  tile->print(); 
  std::cout << "\n";
  // Assign a dereferenced cell iterator to a CSV line
  // This way the return type of operator * of Tile::const_iterator is
  // properly resolved.
  Tile::const_iterator cell_it = tile->begin();
  CSVLine line_7 = *cell_it;  
  line_6 << line_7; // Append first cell of tile to line_6

  // NOTE: The following would not work (compilation error)
  //   CSVLine line_7;
  //   line_7 = *cell_it; 
  // This is because operator = of CSVLine does not recognize 
  // Tile::const_iterator. Follow the appproach outlined above
  // (i.e., always assign *cell_it upon the definition of a CSVLine variable).

  // Alternatively, use the iterator as a stream to ouput into the csv_line
  cell_it++;
  cell_it >> line_6; // Append second cell of tile to line_6
  
  std::cout << "Updated CSV Line: " << line_6.str() << "\n\n";
  delete tile;

  // ------------- //
  // CSVFile usage //
  // ------------- //
  std::cout <<"Creating CSV file...\n\n";
  // The last parameter below is optional (segment size in bytes)
  CSVFile* file = new CSVFile("test.csv", CSVFile::WRITE, 25);
  // We treat the CSVFile object as an output stream
  *file << line_1;
  *file << line_2;
  *file << line_3; // Even comment lines will be written
  *file << line_4;
  *file << line_5;
  // Always delete the CSVFile object when done, because there
  // may still be some data in the buffer that are not flushed
  // yet in the file (deleting the object forces the flushing)
  delete file;

  // Let's print the file
  std::cout << "Printing CSV File (comment lines are ignored):\n";
  CSVFile file_2("test.csv", CSVFile::READ, 25);

  // Note: the comment lines (starting with '#' are ignored)
  while(file_2 >> line)
    std::cout << line.str() << "\n";

  return 0;
}
