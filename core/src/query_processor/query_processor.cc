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
******************** CONSTRUCTORS *********************
******************************************************/

QueryProcessor::QueryProcessor(StorageManager* storage_manager) 
    : storage_manager_(storage_manager) {
  // Success code
  err_ = 0;
}

/******************************************************
********************* ACCESSORS ************************
******************************************************/

int QueryProcessor::err() const {
  return err_;
}

/******************************************************
****************** QUERY FUNCTIONS ********************
******************************************************/

int QueryProcessor::export_csv(
    const std::string& array_name, 
    const std::string& filename, 
    const std::vector<std::string>& dim_names,
    const std::vector<std::string>& attribute_names,
    bool reverse) const {
  std::string err_msg;

  // Open array in read mode
  int ad = storage_manager_->open_array(array_name, "r");
  if(ad == -1) 
    return -1;

  // For easy reference
  int err;
  const ArraySchema* array_schema;
  err = storage_manager_->get_array_schema(ad, array_schema); 
  if(err == -1)
    return -1;
  int attribute_num = array_schema->attribute_num();
  const std::type_info& coords_type = *(array_schema->type(attribute_num));

  // Get dimension and attribute ids
  std::vector<int> dim_ids;
  // TODO: Replace with array_schema->get_dim_ids()
  err = parse_dim_names(dim_names, array_schema, dim_ids, err_msg);
  if(err == -1)
    return -1;
  std::vector<int> attribute_ids;
  // TODO: Replace with array_schema->get_attribute_ids()
  err = parse_attribute_names(attribute_names, array_schema, 
                              attribute_ids, err_msg);
  if(err == -1)
    return -1;

  // Invoke the proper templated function
  if(coords_type == typeid(int)) { 
    if(!reverse)
      export_csv<int>(ad, array_schema, filename, 
                      dim_ids, attribute_ids); 
    else
      export_csv_reverse<int>(ad, array_schema, filename, 
                              dim_ids, attribute_ids);
  } else if(coords_type == typeid(int64_t)) {

    if(!reverse)
      export_csv<int64_t>(ad, array_schema, filename, 
                          dim_ids, attribute_ids);
    else
      export_csv_reverse<int64_t>(ad, array_schema, filename, 
                                  dim_ids, attribute_ids); 
  } else if(coords_type == typeid(float)) {
    if(!reverse)
      export_csv<float>(ad, array_schema, filename, 
                        dim_ids, attribute_ids); 
    else
      export_csv_reverse<float>(ad, array_schema, filename, 
                                dim_ids, attribute_ids); 
  } else if(coords_type == typeid(double)) {
    if(!reverse)
      export_csv<double>(ad, array_schema, filename, 
                         dim_ids, attribute_ids); 
    else
      export_csv_reverse<double>(ad, array_schema, filename, 
                                 dim_ids, attribute_ids); 
  }

  // Clean up
  storage_manager_->close_array(ad); 

  return TILEDB_OK;
}

int QueryProcessor::subarray(
    const std::string& array_name, 
    const std::vector<double>& range,
    const std::string& result_array_name,
    const std::vector<std::string>& attribute_names) const {
  std::string err_msg;

  // Open array in read mode
  int ad = storage_manager_->open_array(array_name, "r");
  if(ad == -1)
    return -1;

  // For easy reference
  const ArraySchema* array_schema;
  int err;
  err = storage_manager_->get_array_schema(ad, array_schema); 
  if(err == -1)
    return -1;
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  const std::type_info& coords_type = *(array_schema->type(attribute_num));

  // Check range size
  if(range.size() != 2*dim_num) {
    err_msg = "Range dimensionality does not agree with"
              " number of dimensions in the array schema";
    return -1;
  }

  // The attributes of the input array cannot be hidden in the result
  if(attribute_names.size() == 1 && attribute_names[0] == "__hide") { 
    err_msg = "Attribute names cannot be hidden in a subarray query.";
    return -1;
  }

  // Get attribute ids
  std::vector<int> attribute_ids; 
  err = parse_attribute_names(attribute_names, array_schema, 
                              attribute_ids, err_msg);
  if(err == -1)
    return -1;
  assert(no_duplicates(attribute_ids));

  // Create result array schema
  ArraySchema* result_array_schema = array_schema->clone(result_array_name, 
                                                         attribute_ids); 

  // Define result array
  err = storage_manager_->define_array(result_array_schema);
  if(err == -1)
    return -1;

  // Open result array in write mode
  int result_ad = storage_manager_->open_array(result_array_name, "w");
  if(result_ad == -1)
    return -1;

  // Invoke the proper templated function
  if(coords_type == typeid(int)) { 
    int* new_range = new int[2*dim_num]; 
    convert(&range[0], new_range, 2*dim_num);
    subarray(ad, new_range, result_ad, attribute_ids); 
    delete [] new_range;
  } else if(coords_type == typeid(int64_t)) { 
    int64_t* new_range = new int64_t[2*dim_num]; 
    convert(&range[0], new_range, 2*dim_num);
    subarray(ad, new_range, result_ad, attribute_ids); 
    delete [] new_range;
 } else if(coords_type == typeid(float)) { 
    float* new_range = new float[2*dim_num]; 
    convert(&range[0], new_range, 2*dim_num);
    subarray(ad, new_range, result_ad, attribute_ids); 
    delete [] new_range;
  } else if(coords_type == typeid(double)) { 
    double* new_range = new double[2*dim_num]; 
    convert(&range[0], new_range, 2*dim_num);
    subarray(ad, new_range, result_ad, attribute_ids); 
    delete [] new_range;
  }

  // Clean up
  storage_manager_->close_array(ad); 
  storage_manager_->close_array(result_ad); 
  delete result_array_schema;

  return TILEDB_OK;
}

/******************************************************
******************** PRIVATE METHODS ******************
******************************************************/

template<class T>
void QueryProcessor::export_csv(
    int ad,
    const ArraySchema* array_schema,
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids) const {
  // Prepare CSV file
  CSVFile csv_file(filename, "w");

  // Prepare cell iterators
  ArrayConstCellIterator<T>* cell_it = 
      storage_manager_->begin<T>(ad, attribute_ids);

  // Prepare a cell
  Cell cell(array_schema, cell_it->attribute_ids(), 0, true);

  // Write cells into the CSV file
  for(int i=0; !cell_it->end(); ++(*cell_it), ++i) { 
    cell.set_cell(**cell_it);
    csv_file << cell.csv_line<T>(dim_ids, attribute_ids); 
  }

  // Clean up 
  csv_file.close();
}

template<class T>
void QueryProcessor::export_csv_reverse(
    int ad, const ArraySchema* array_schema, 
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids) const {
  // Prepare CSV file
  CSVFile csv_file;
  csv_file.open(filename, "w");

  // Prepare cell iterators
  ArrayConstReverseCellIterator<T>* cell_it = 
      storage_manager_->rbegin<T>(ad, attribute_ids);

  // Prepare a cell
  Cell cell(array_schema, cell_it->attribute_ids(), 0, true);

  // Write cells into the CSV file
  for(; !cell_it->end(); ++(*cell_it)) { 
    cell.set_cell(**cell_it);
    csv_file << cell.csv_line<T>(dim_ids, attribute_ids); 
  }

  // Clean up 
  csv_file.close();
}

int QueryProcessor::parse_attribute_names(
   const std::vector<std::string>& attribute_names,
   const ArraySchema* array_schema,
   std::vector<int>& attribute_ids, std::string& err_msg) const {
  // Initialization
  attribute_ids.clear();

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
        if(attribute_ids[i] == -1) {
          err_msg = std::string("Invalid attribute name ") + 
                    attribute_names[i] + ".";
          return -1;
        }
      }
    }
  }

  return TILEDB_OK;
}

int QueryProcessor::parse_dim_names(
   const std::vector<std::string>& dim_names,
   const ArraySchema* array_schema, 
   std::vector<int>& dim_ids, std::string& err_msg) const {
  // Initialization
  dim_ids.clear();

  // Special case for "hide dimensions"
  if(dim_names.size() == 1 && dim_names[0] == "__hide")
    return TILEDB_OK;

  // For easy reference
  int dim_num = array_schema->dim_num();

  if(dim_names.size() == 0) {
    dim_ids.resize(dim_num);
    for(int i=0; i<dim_num; ++i)
      dim_ids[i] = i;
  } else {
    dim_ids.resize(dim_names.size());
    for(int i=0; i<dim_names.size(); ++i) {
      dim_ids[i] = array_schema->dim_id(dim_names[i]);
      if(dim_ids[i] == -1) {
        err_msg = std::string("Invalid dimension name ") +
                  dim_names[i] + ".";
        return -1;
      }
    }
  }

  return TILEDB_OK;
} 

template<class T>
void QueryProcessor::subarray(
    int ad, const T* range, int result_ad,
    const std::vector<int>& attribute_ids) const { 
  // Prepare cell iterator
  ArrayConstCellIterator<T>* cell_it = 
      storage_manager_->begin<T>(ad, range, attribute_ids);

  // Write cells into the CSV file
  for(; !cell_it->end(); ++(*cell_it)) 
    storage_manager_->write_cell_sorted<T>(result_ad, **cell_it); 
}

