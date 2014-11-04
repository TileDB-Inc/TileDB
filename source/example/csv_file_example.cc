#include "csv_file.h"
#include <iostream>

int main() {
  // ------- CSVLine USAGE -------
  CSVLine line_1, line_2, line_3, line_4;
 
  // We can assign a CSV string (or const char*) to a CSVLine object 
  line_1 = "10,5.1,stavros";
  // We can treat a CSVLine object as an output stream
  // Several data types are supported
  line_2 << 5;
  line_2 << 3.7;
  line_2 << "papadopoulos";
  // We can assign a string and then keep appending data
  line_3 = "11,4.0";
  line_3 << "TileDB";

  // Let's view the lines
  std::cout << "Printing CSV lines:\n";  
  std::cout << line_1.str() << "\n";
  std::cout << line_2.str() << "\n";
  std::cout << line_3.str() << "\n";

  // ------- CSVFile USAGE -------
  CSVFile* file = new CSVFile("test.csv", CSVFile::WRITE, 25);
  // We treate the CSVFile object as an output stream
  *file << line_1;
  *file << line_2;
  *file << line_3;
  // Always delete the CSVFile object when done, because there
  // may still be some data in the buffer that are not flushed
  // yet in the file (deleting the object forces the flushing)
  delete file;

  // Let's print the file
  std::cout << "Printing CSV File:\n";
  CSVFile file_2("test.csv", CSVFile::READ, 25);

  try {
    while(file_2 >> line_4)
      std::cout << line_4.str() << "\n";
  } catch(CSVFileException& e) {
    std::cout << e.what() << "\n";
  }

  // Let's print it again, this time iterating over the values of the lines
  // We assume that the format of each line is (int),(float),(string)
  std::cout << "Printing CSV File (again):\n";
  CSVFile file_3("test.csv", CSVFile::READ, 25);
 
  int i;
  float f;
  std::string s;
  try {
    while(file_3 >> line_4) {
      line_4 >> i;
      line_4 >> f;
      line_4 >> s;
      std::cout << i << "," << f << "," << s << "\n";
    }
  } catch(CSVFileException& e) {
    std::cout << e.what() << "\n";
  }

  // We will print a last time, scanning this time the individual values
  // of each CSV line (treating every value as a string)
  std::cout << "Printing CSV File (last time):\n";
  CSVFile file_4("test.csv", CSVFile::READ, 25);
 
  try {
    while(file_4 >> line_4) {
      if(line_4 >> s)
        std::cout << s;
      while(line_4 >> s)
        std::cout << "," << s;
      std::cout << "\n";
    }
  } catch(CSVFileException& e) {
    std::cout << e.what() << "\n";
  }

  return 0;
}
