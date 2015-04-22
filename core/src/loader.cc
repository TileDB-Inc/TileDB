/**
 * @file   loader.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file implements the Loader class.
 */

#include "loader.h"
#include "utils.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <iostream>
#include <sstream>

/******************************************************
********************* CONSTRUCTORS ********************
******************************************************/

Loader::Loader(StorageManager* storage_manager) 
    : storage_manager_(storage_manager) {
}

/******************************************************
******************* LOADING FUNCTIONS *****************
******************************************************/

void Loader::load_csv(const std::string& filename, 
                      const std::string& array_name) const {
  // Open array in write mode
  int ad = storage_manager_->open_array(array_name, "w");
  if(ad == -1)
    throw LoaderException(std::string("Cannot open array ") +
                          array_name + "."); 

  // Open the csv file 
  CSVLine csv_line;
  CSVFile csv_file;
  if(!csv_file.open(filename, "r")) {
    // Clean up
    storage_manager_->close_array(ad);
    // TODO: Delete last fragment (force_close_array) 
 
    // Throw exception
    throw LoaderException(std::string("Cannot open file ") +
                          filename + "."); 
  }

  // For easy reference
  const ArraySchema* array_schema = storage_manager_->get_array_schema(ad);
  size_t cell_size = array_schema->cell_size();
  int64_t line = 0;

  // Prepare a cell buffer
  void* cell = malloc(cell_size);

  while(csv_file >> csv_line) {
    ++line;

    // Get a logical cell
    if(!csv_line_to_cell(array_schema, csv_line, cell)) { 
      // Clean up
      storage_manager_->close_array(ad); 
      // TODO: Delete last fragment  (force_close_array)
      csv_file.close();
      free(cell);

      // Throw exception
      std::stringstream ss;
      ss << "Cannot load cell from line " << line 
         << " of file " << filename << ".";
      throw LoaderException(ss.str());
    }

    // Write the cell in the array
    storage_manager_->write_cell(ad, cell);
  }

  // Clean up 
  storage_manager_->close_array(ad); 
  csv_file.close();
  free(cell);
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

template<class T>
bool Loader::get_attribute(CSVLine& csv_line, void* cell) const {

  T v;
  if(!(csv_line >> v))
    return false;

  memcpy(cell, &v, sizeof(T));
 
  return true; 
}

template<class T>
bool Loader::get_coordinates(CSVLine& csv_line, 
                                void* cell, int dim_num) const {
  T* coords = new T[dim_num];

  for(int i=0; i<dim_num; ++i) {
    if(!(csv_line >> coords[i])) {
      delete [] coords;
      return false;
    }
  }

  memcpy(cell, coords, dim_num*sizeof(T));

  delete [] coords;

  return true;
}

inline
bool Loader::csv_line_to_cell(const ArraySchema* array_schema, 
                      CSVLine& csv_line, void* cell) const {
  // For easy reference
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();

  bool success;
  size_t offset = 0;

  // Append coordinates
  if(*(array_schema->type(attribute_num)) == typeid(int))
    success = get_coordinates<int>(csv_line, cell, dim_num); 
  else if(*(array_schema->type(attribute_num)) == typeid(int64_t))
    success = get_coordinates<int64_t>(csv_line, cell, dim_num); 
  else if(*(array_schema->type(attribute_num)) == typeid(float))
    success = get_coordinates<float>(csv_line, cell, dim_num); 
  else if(*(array_schema->type(attribute_num)) == typeid(double))
    success = get_coordinates<double>(csv_line, cell, dim_num); 

  // Append attribute values
  if(success) {
    offset += array_schema->cell_size(attribute_num);

    for(int i=0; i<attribute_num; ++i) {
      if(*(array_schema->type(i)) == typeid(char))
        success = get_attribute<char>(csv_line, 
                                      static_cast<char*>(cell) + offset);
      else if(*(array_schema->type(i)) == typeid(int))
        success = get_attribute<int>(csv_line,
                                     static_cast<char*>(cell) + offset);
      else if(*(array_schema->type(i)) == typeid(int64_t))
        success = get_attribute<int64_t>(csv_line, 
                                         static_cast<char*>(cell) + offset);
      else if(*(array_schema->type(i)) == typeid(float))
        success = get_attribute<float>(csv_line, 
                                       static_cast<char*>(cell) + offset);
      else if(*(array_schema->type(i)) == typeid(double))
        success = get_attribute<double>(csv_line, 
                                        static_cast<char*>(cell) + offset);
      if(!success)
        break;
  
      offset += array_schema->cell_size(i);
    }
  }

  return success;
}
