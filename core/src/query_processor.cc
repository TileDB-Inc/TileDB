/**
 * @file   query_processor.cc
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
 * This file implements the QueryProcessor class.
 */
  
#include "query_processor.h"
#include "utils.h"
#include "cell.h"
#include <stdio.h>
#include <typeinfo>
#include <stdlib.h>
#include <iostream>
#include <sys/stat.h>
#include <sys/uio.h>
#include <assert.h>
#include <algorithm>
#include <functional>
#include <math.h>
#include <string.h>

/******************************************************
********************* CONSTRUCTORS ********************
******************************************************/

QueryProcessor::QueryProcessor(StorageManager* storage_manager) 
    : storage_manager_(storage_manager) {
}

void QueryProcessor::export_to_csv(
    const std::string& array_name, 
    const std::string& filename, 
    const std::vector<std::string>& dim_names,
    const std::vector<std::string>& attribute_names,
    bool reverse) const {
  // Open array in read mode
  int ad = storage_manager_->open_array(array_name, SM_READ_MODE);
  if(ad == -1)
    throw QueryProcessorException(std::string("Cannot open array ") +
                                  array_name + "."); 

  // For easy reference
  const ArraySchema* array_schema = storage_manager_->get_array_schema(ad); 
  int attribute_num = array_schema->attribute_num();
  const std::type_info& coords_type = *(array_schema->type(attribute_num));

  // Get dimension and attribute ids
  std::vector<int> dim_ids = parse_dim_names(dim_names, array_schema);
  std::vector<int> attribute_ids = parse_attribute_names(attribute_names, 
                                                         array_schema);

  // Invoke the proper templated function
  if(coords_type == typeid(int)) { 
    if(!reverse)
      export_to_csv<int>(ad, filename, dim_ids, attribute_ids); 
    else
      export_to_csv_reverse<int>(ad, filename, dim_ids, attribute_ids); 
  } else if(coords_type == typeid(int64_t)) {
    if(!reverse)
      export_to_csv<int64_t>(ad, filename, dim_ids, attribute_ids); 
    else
      export_to_csv_reverse<int64_t>(ad, filename, dim_ids, attribute_ids); 
  } else if(coords_type == typeid(float)) {
    if(!reverse)
      export_to_csv<float>(ad, filename, dim_ids, attribute_ids); 
    else
      export_to_csv_reverse<float>(ad, filename, dim_ids, attribute_ids); 
  } else if(coords_type == typeid(double)) {
    if(!reverse)
      export_to_csv<double>(ad, filename, dim_ids, attribute_ids); 
    else
      export_to_csv_reverse<double>(ad, filename, dim_ids, attribute_ids); 
  }

  // Clean up
  storage_manager_->close_array(ad); 
}

void QueryProcessor::subarray(
    const std::string& array_name, 
    const std::vector<double>& range,
    const std::string& result_array_name,
    const std::vector<std::string>& attribute_names, 
    bool reverse) const {
  // Open array in read mode
  int ad = storage_manager_->open_array(array_name, SM_READ_MODE);
  if(ad == -1)
    throw QueryProcessorException(std::string("Cannot open array ") +
                                  array_name + "."); 

  // For easy reference
  const ArraySchema* array_schema = storage_manager_->get_array_schema(ad); 
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  const std::type_info& coords_type = *(array_schema->type(attribute_num));

  // Check range size
  if(range.size() != 2*dim_num)
    throw QueryProcessorException("Range dimensionality does not agree with"
                                  " number of dimensions in the array schema");

  // The attributes of the input array cannot be hidden in the result
  if(attribute_names.size() == 1 && attribute_names[0] == "__hide") 
    throw QueryProcessorException("Attribute names cannot be"
                                  " hidden in a subarray query.");

  // Get attribute ids
  std::vector<int> attribute_ids = 
      parse_attribute_names(attribute_names, array_schema);
  assert(no_duplicates(attribute_ids));

  // Create result array schema
  ArraySchema* result_array_schema = array_schema->clone(result_array_name, 
                                                         attribute_ids); 

  // Define result array
  storage_manager_->define_array(result_array_schema);

  // Open result array in write mode
  int result_ad = storage_manager_->open_array(result_array_name, 
                                               SM_WRITE_MODE);
  if(result_ad == -1)
    throw QueryProcessorException(std::string("Cannot open array ") +
                                  result_array_name + "."); 

  // Invoke the proper templated function
  if(coords_type == typeid(int)) { 
    int* new_range = new int[2*dim_num]; 
    convert(&range[0], new_range, 2*dim_num);
    if(!reverse)
      subarray(ad, new_range, result_ad, attribute_ids); 
    else
      subarray_reverse(ad, new_range, result_ad, attribute_ids); 
    delete [] new_range;
  } else if(coords_type == typeid(int64_t)) { 
    int64_t* new_range = new int64_t[2*dim_num]; 
    convert(&range[0], new_range, 2*dim_num);
    if(!reverse)
      subarray(ad, new_range, result_ad, attribute_ids); 
    else
      subarray_reverse(ad, new_range, result_ad, attribute_ids); 
    delete [] new_range;
 } else if(coords_type == typeid(float)) { 
    float* new_range = new float[2*dim_num]; 
    convert(&range[0], new_range, 2*dim_num);
    if(!reverse)
      subarray(ad, new_range, result_ad, attribute_ids); 
    else
      subarray_reverse(ad, new_range, result_ad, attribute_ids); 
    delete [] new_range;
  } else if(coords_type == typeid(double)) { 
    double* new_range = new double[2*dim_num]; 
    convert(&range[0], new_range, 2*dim_num);
    if(!reverse)
      subarray(ad, new_range, result_ad, attribute_ids); 
    else
      subarray_reverse(ad, new_range, result_ad, attribute_ids); 
    delete [] new_range;
  }

  // Clean up
  storage_manager_->close_array(ad); 
  storage_manager_->close_array(result_ad); 
  delete result_array_schema;
}

/******************************************************
******************** PRIVATE METHODS ******************
******************************************************/

template<class T>
void QueryProcessor::export_to_csv(
    int ad, 
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids) const {
  // For easy reference
  const ArraySchema* array_schema = storage_manager_->get_array_schema(ad); 

  // Prepare CSV file
  CSVFile csv_file;
  csv_file.open(filename, "w");

  // Prepare cell iterators
  StorageManager::Array::const_cell_iterator<T> cell_it = 
      storage_manager_->begin<T>(ad, attribute_ids);

  // Prepare a cell
  Cell cell(array_schema, cell_it.attribute_ids(), true);

  // Write cells into the CSV file
  for(; !cell_it.end(); ++cell_it) { 
    cell.set_cell(*cell_it);
    csv_file << cell.csv_line<T>(dim_ids, attribute_ids); 
  }

  // Clean up 
  csv_file.close();
}

template<class T>
void QueryProcessor::export_to_csv_reverse(
    int ad, 
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids) const {
  // For easy reference
  const ArraySchema* array_schema = storage_manager_->get_array_schema(ad); 

  // Prepare CSV file
  CSVFile csv_file;
  csv_file.open(filename, "w");

  // Prepare cell iterators
  StorageManager::Array::const_reverse_cell_iterator<T> cell_it = 
      storage_manager_->rbegin<T>(ad, attribute_ids);

  // Prepare a cell
  Cell cell(array_schema, cell_it.attribute_ids(), true);

  // Write cells into the CSV file
  for(; !cell_it.end(); ++cell_it) { 
    cell.set_cell(*cell_it);
    csv_file << cell.csv_line<T>(dim_ids, attribute_ids); 
  }

  // Clean up 
  csv_file.close();
}

std::vector<int> QueryProcessor::parse_attribute_names(
   const std::vector<std::string>& attribute_names,
   const ArraySchema* array_schema) const {
  std::vector<int> attribute_ids;

  // If "hide attributes" is selected, the return list should be empty
  if(attribute_names.size() == 0 || attribute_names[0] != "__hide") {
    // For easy reference
    int attribute_num = array_schema->attribute_num();
  
    // Default ids in case the name list is empty
    if(attribute_names.size() == 0) {
      attribute_ids.resize(attribute_num);
      for(int i=0; i<attribute_num; ++i)
        attribute_ids[i] = i;
    } else { // Ids retrieved from the name list
      attribute_ids.resize(attribute_names.size());
      for(int i=0; i<attribute_names.size(); ++i) {
        attribute_ids[i] = array_schema->attribute_id(attribute_names[i]);
        if(attribute_ids[i] == -1)
          throw QueryProcessorException(std::string("Invalid attribute name ") +
                                        attribute_names[i] + "."); 
      }
    }
  }

  return attribute_ids;
}

std::vector<int> QueryProcessor::parse_dim_names(
   const std::vector<std::string>& dim_names,
   const ArraySchema* array_schema) const {
  // Special case for "hide dimensions"
  if(dim_names.size() == 1 && dim_names[0] == "__hide")
    return std::vector<int>();

  // For easy reference
  int dim_num = array_schema->dim_num();

  std::vector<int> dim_ids;
  if(dim_names.size() == 0) {
    dim_ids.resize(dim_num);
    for(int i=0; i<dim_num; ++i)
      dim_ids[i] = i;
  } else {
    dim_ids.resize(dim_names.size());
    for(int i=0; i<dim_names.size(); ++i) {
      dim_ids[i] = array_schema->dim_id(dim_names[i]);
      if(dim_ids[i] == -1)
        throw QueryProcessorException(std::string("Invalid dimension name ") +
                                      dim_names[i] + "."); 
    }
  }

  return dim_ids;
} 

template<class T>
void QueryProcessor::subarray(
    int ad, const T* range, int result_ad,
    const std::vector<int>& attribute_ids) const { 
  // Prepare cell iterator
  StorageManager::Array::const_cell_iterator<T> cell_it = 
      storage_manager_->begin<T>(ad, range, attribute_ids);

  // Write cells into the CSV file
  for(; !cell_it.end(); ++cell_it) 
    storage_manager_->write_cell_sorted<T>(result_ad, *cell_it); 
}

template<class T>
void QueryProcessor::subarray_reverse(
    int ad, const T* range, int result_ad,
    const std::vector<int>& attribute_ids) const { 
  // Prepare cell iterator
  StorageManager::Array::const_reverse_cell_iterator<T> cell_it = 
      storage_manager_->rbegin<T>(ad, range, attribute_ids);

  // Write cells into the CSV file
  for(; !cell_it.end(); ++cell_it) 
    storage_manager_->write_cell_sorted<T>(result_ad, *cell_it); 
}
