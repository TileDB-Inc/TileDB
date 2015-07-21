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

#include "bin_file.h"
#include "bin_file_collection.h"
#include "cell.h"
#include "csv_file.h"
#include "csv_file_collection.h"
#include "loader.h"
#include "special_values.h"
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
************* CONSTRUCTORS & DESTRUCTORS***************
******************************************************/

Loader::Loader(StorageManager* storage_manager, const std::string& path)
    : storage_manager_(storage_manager) {
  // Success code
  err_ = 0;

  if(!is_dir(path)) {
    std::cerr << ERROR_MSG_HEADER 
              << " Workspace directory '" << path << "' does not exist.\n";
    err_ = TILEDB_EDNEXIST;
    return;
  }

  // Set workspace
  set_workspace(path);

  // Create directories
  int rc;
  if(!is_dir(workspace_)) {
    rc = create_directory(workspace_);
    if(rc) {
      std::cerr << ERROR_MSG_HEADER
                << " Cannot create directory '" << workspace_ << "'.\n";
      err_ = TILEDB_EDNCREAT;
      return;
    }
  }
}

Loader::~Loader() {
}

/******************************************************
********************* ACCESSORS ************************
******************************************************/

int Loader::err() const {
  return err_;
}

/******************************************************
******************* LOADING FUNCTIONS *****************
******************************************************/

int Loader::load_bin(
    const std::string& array_name, 
    const std::string& path,
    bool sorted) const {

  // TODO fix error messages

  // Open array in write mode
  int ad = storage_manager_->open_array(array_name, "w");
  if(ad == -1) {
    return -1;
  }

  // For easy reference
  const ArraySchema* array_schema;
  int err = storage_manager_->get_array_schema(ad, array_schema);
  if(err == -1)
    return -1;
  int attribute_num = array_schema->attribute_num();
  const std::type_info& coords_type = *(array_schema->type(attribute_num));

  if(coords_type == typeid(int))
    return load_bin<int>(ad, path, sorted);
  else if(coords_type == typeid(int64_t))
    return load_bin<int64_t>(ad, path, sorted);
  else if(coords_type == typeid(float))
    return load_bin<float>(ad, path, sorted);
  else if(coords_type == typeid(double))
    return load_bin<double>(ad, path, sorted);
  else
    assert(false); // this shoud not happen
  return -1;
}

int Loader::load_csv(
    const std::string& array_name, 
    const std::string& path,
    bool sorted) const {
  // TODO fix error messages

  // Open array in write mode
  int ad = storage_manager_->open_array(array_name, "w");
  if(ad == -1) { 
    return -1;
  }

  // For easy reference
  const ArraySchema* array_schema;
  int err = storage_manager_->get_array_schema(ad, array_schema);
  if(err == -1)
    return -1;
  int attribute_num = array_schema->attribute_num();
  const std::type_info& coords_type = *(array_schema->type(attribute_num));

  if(coords_type == typeid(int))
    return load_csv<int>(ad, path, sorted);
  else if(coords_type == typeid(int64_t))
    return load_csv<int64_t>(ad, path, sorted);
  else if(coords_type == typeid(float))
    return load_csv<float>(ad, path, sorted);
  else if(coords_type == typeid(double))
    return load_csv<double>(ad, path, sorted);
  else
      assert(false); // this should not happen
  return -1;
}

int Loader::update_bin(
    const std::string& array_name, 
    const std::string& path,
    bool sorted) const {

  // TODO fix error messages

  // Open array in write mode
  int ad = storage_manager_->open_array(array_name, "a");
  if(ad == -1) { 
    return -1;
  }

  // For easy reference
  const ArraySchema* array_schema;
  int err = storage_manager_->get_array_schema(ad, array_schema);
  if(err == -1)
    return -1;
  int attribute_num = array_schema->attribute_num();
  const std::type_info& coords_type = *(array_schema->type(attribute_num));

  if(coords_type == typeid(int))
    return load_bin<int>(ad, path, sorted);
  else if(coords_type == typeid(int64_t))
    return load_bin<int64_t>(ad, path, sorted);
  else if(coords_type == typeid(float))
    return load_bin<float>(ad, path, sorted);
  else if(coords_type == typeid(double))
    return load_bin<double>(ad, path, sorted);
  else
    assert(false); // this should not happen
  return -1;
}

int Loader::update_csv(
    const std::string& array_name, 
    const std::string& path,
    bool sorted) const {
  // TODO fix error messages

  // Open array in write mode
  int ad = storage_manager_->open_array(array_name, "a");
  if(ad == -1) { 
    return -1;
  }

  // For easy reference
  const ArraySchema* array_schema;
  int err = storage_manager_->get_array_schema(ad, array_schema);
  if(err == -1)
    return -1;
  int attribute_num = array_schema->attribute_num();
  const std::type_info& coords_type = *(array_schema->type(attribute_num));

  if(coords_type == typeid(int))
    return load_csv<int>(ad, path, sorted);
  else if(coords_type == typeid(int64_t))
    return load_csv<int64_t>(ad, path, sorted);
  else if(coords_type == typeid(float))
    return load_csv<float>(ad, path, sorted);
  else if(coords_type == typeid(double))
    return load_csv<double>(ad, path, sorted);
  else
    assert(false); // this should not happen
  return -1;
}

int Loader::load_bin(
    const std::string& filename, const std::string& array_name) const {
  // Open array in write mode
  int ad = storage_manager_->open_array(array_name, "w");
  if(ad == -1) { 
    return -1;
  }

  // Load binary file
  int err = load_bin(filename, ad); 

  // Clean up
  storage_manager_->close_array(ad); 

  return err;
}

int Loader::load_csv(
    const std::string& array_name,
    const std::string& filename) const {
  // Open array in write mode
// TODO: it should return an error code (failed to open or 
// undefined)
  int ad = storage_manager_->open_array(array_name, "w");
  if(ad == -1) { 
    std::cerr << ERROR_MSG_HEADER 
              << " Failed to open array '" << array_name << "'.\n";
    return TILEDB_EOARR;
  }

  // Load CSV file
  int err = load_csv(ad, filename);
 
  // Clean up
  storage_manager_->close_array(ad);

  return err;
}

int Loader::load_sorted_bin(
    const std::string& filename, const std::string& array_name) const {
  // Open array in write mode
  int ad = storage_manager_->open_array(array_name, "w");
  if(ad == -1) 
    return -1;

  // Load CSV file
  int err = load_sorted_bin(filename, ad); 

  // Clean up
  storage_manager_->close_array(ad); 

  return err;
}

int Loader::update_csv(
    const std::string& filename, const std::string& array_name,
    std::string& err_msg) const {
  // Open array in append mode
  int ad = storage_manager_->open_array(array_name, "a");
  if(ad == -1) 
    return -1;

  // Load CSV file
  int err = load_csv(ad, filename); 

  // Clean up
  storage_manager_->close_array(ad);

  return err;
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

template<class T>
bool Loader::append_attribute(
    CSVLine& csv_line, int val_num, void* cell, size_t& offset) const {
  T v;
  char* c_cell = static_cast<char*>(cell);

  if(val_num != VAR_SIZE) { // Fixed-sized attribute 
    for(int i=0; i<val_num; ++i) {
      if(!(csv_line >> v))
        return false;
      memcpy(c_cell + offset, &v, sizeof(T));
      offset += sizeof(T);
    }
  } else {                  // Variable-sized attribute 
    if(typeid(T) == typeid(char)) {
      std::string s;
      if(!(csv_line >> s))
        return false;
      int s_size = s.size();
      memcpy(c_cell + offset, &s_size, sizeof(int));
      offset += sizeof(int);
      memcpy(c_cell + offset, s.c_str(), s.size());
      offset += s.size(); 
    } else {
      int num;
      if(!(csv_line >> num))
        return false;
      memcpy(c_cell + offset, &num, sizeof(int));
      offset += sizeof(int);
      for(int i=0; i<num; ++i) {
        if(!(csv_line >> v))
          return false;
        memcpy(c_cell + offset, &v, sizeof(T));
        offset += sizeof(T);
      }
    }
  }
 
  return true; 
}

template<class T>
bool Loader::append_coordinates(CSVLine& csv_line, 
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

// --- Cell format ---
// coordinates, cell_size, 
//	attribute#1_value#1, ...            (fixed-sized attribute)
// 	val_num, attribute#2_value#1,...,   (variable-sized attribute)
ssize_t Loader::calculate_cell_size(
    CSVLine& csv_line, const ArraySchema* array_schema) const {
  // For easy reference
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();

  // Initialize cell size - it will be updated below
  ssize_t cell_size = array_schema->cell_size(attribute_num) + sizeof(size_t);
  // Skip the coordinates in the CSV line
  csv_line += dim_num; 
 
  for(int i=0; i<attribute_num; ++i) {
    if(array_schema->cell_size(i) != VAR_SIZE) { // Fixed-sized attribute
      cell_size += array_schema->cell_size(i);
      csv_line += array_schema->val_num(i);
    } else {                                     // Variable-sized attribute
      if(array_schema->type(i) == &typeid(char)) {
        cell_size += sizeof(int) + (*csv_line).size()*sizeof(char);
        ++csv_line;
      } else {
        int num;
        if(!(csv_line >> num))
          return -1; // Error

        if(array_schema->type(i) == &typeid(int)) 
          cell_size += sizeof(int) + num*sizeof(int);
        else if(array_schema->type(i) == &typeid(float)) 
          cell_size += sizeof(int) + num*sizeof(float);
        else if(array_schema->type(i) == &typeid(double)) 
          cell_size += sizeof(int) + num*sizeof(double);

        csv_line += num;
      }
    }
  }

  // Reset the position of the CSV line to the beginning
  csv_line.reset();

  return cell_size;
}

// --- Cell format ---
// coordinates, cell_size, 
//	attribute#1_value#1, ...            (fixed-sized attribute)
// 	val_num, attribute#2_value#1,...,   (variable-sized attribute)
template<class T>
inline
bool Loader::csv_line_to_cell(
    const ArraySchema* array_schema, CSVLine& csv_line, void* cell, 
    ssize_t cell_size) const {
  // For easy reference
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();

  bool success;
  size_t offset = 0;

  // Append coordinates
  success = append_coordinates<T>(csv_line, cell, dim_num); 
  offset += array_schema->cell_size(attribute_num);

  if(success) {
    // Append cell size 
    if(array_schema->cell_size() == VAR_SIZE) {
      memcpy(static_cast<char*>(cell) + offset, &cell_size, sizeof(size_t));
      offset += sizeof(size_t);
    }

    // Append attribute values
    for(int i=0; i<attribute_num; ++i) {
      if(*(array_schema->type(i)) == typeid(char))
        success = append_attribute<char>(
                      csv_line, array_schema->val_num(i), 
                      cell, offset);
      else if(*(array_schema->type(i)) == typeid(int))
        success = append_attribute<int>(
                      csv_line, array_schema->val_num(i), 
                      cell, offset);
      else if(*(array_schema->type(i)) == typeid(int64_t))
        success = append_attribute<int64_t>( 
                      csv_line, array_schema->val_num(i), 
                      cell, offset);
      else if(*(array_schema->type(i)) == typeid(float))
        success = append_attribute<float>( 
                      csv_line, array_schema->val_num(i), 
                      cell, offset);
      else if(*(array_schema->type(i)) == typeid(double))
        success = append_attribute<double>( 
                      csv_line, array_schema->val_num(i), 
                      cell, offset);
      if(!success)
        break;
    }
  }

  return success;
}

int Loader::load_bin(const std::string& filename, int ad) const {
  // For easy reference
  const ArraySchema* array_schema;
  int err = storage_manager_->get_array_schema(ad, array_schema);
  if(err == -1)
    return -1;
  int attribute_num = array_schema->attribute_num();
  const std::type_info& coords_type = *(array_schema->type(attribute_num));

  if(coords_type == typeid(int))
    return load_bin<int>(filename, ad);
  else if(coords_type == typeid(int64_t))
    return load_bin<int64_t>(filename, ad);
  else if(coords_type == typeid(float))
    return load_bin<float>(filename, ad);
  else if(coords_type == typeid(double))
    return load_bin<double>(filename, ad);
  else
      assert(false); // this should not happen
  return -1;
}

template<class T>
int Loader::load_bin(const std::string& filename, int ad) const {
  // For easy reference
  const ArraySchema* array_schema;
  int err = storage_manager_->get_array_schema(ad, array_schema);
  if(err == -1)
    return -1;
  ssize_t cell_size = array_schema->cell_size();
  bool var_size = (cell_size == VAR_SIZE);

  // Open the BIN file 
  BINFile bin_file(array_schema);
  if(bin_file.open(filename, "r") == -1) {
    storage_manager_->forced_close_array(ad);
    return -1;
  }

  // Prepare a cell
  Cell cell(array_schema);

  while(bin_file >> cell) 
    storage_manager_->write_cell<T>(ad, cell.cell());

  // Clean up 
  bin_file.close();
  storage_manager_->close_array(ad);

  return TILEDB_OK;
}

template<class T>
int Loader::load_bin(
    int ad, 
    const std::string& path,
    bool sorted) const {
  // For easy reference
  const ArraySchema* array_schema;
  int err = storage_manager_->get_array_schema(ad, array_schema);
  if(err == -1)
    return -1;

  // Open the BIN file collection 
  BINFileCollection<T> bin_file_collection(workspace_ + "/" + "__temp");
  err = bin_file_collection.open(array_schema, 0, path, sorted);
  if(err)
    return -1;

  // Prepare a cell
  Cell cell(array_schema);

  while(bin_file_collection >> cell) { 
    if(sorted)
      storage_manager_->write_cell_sorted<T>(ad, cell.cell());
    else 
      storage_manager_->write_cell<T>(ad, cell.cell());
  }

  // Clean up
  storage_manager_->close_array(ad);
  err = bin_file_collection.close();
  if(err)
    return -1;

  return TILEDB_OK;
}

template<class T>
int Loader::load_csv(
    int ad, 
    const std::string& path,
    bool sorted) const {
  // For easy reference
  const ArraySchema* array_schema;
  int err = storage_manager_->get_array_schema(ad, array_schema);
  if(err == -1)
    return -1;

  // Open the CSV file collection 
  CSVFileCollection<T> csv_file_collection;
  err = csv_file_collection.open(array_schema, path, sorted);
  if(err)
    return -1;

  // Prepare a cell
  Cell cell(array_schema);

  while(csv_file_collection >> cell) { 
    if(sorted)
      storage_manager_->write_cell_sorted<T>(ad, cell.cell());
    else 
      storage_manager_->write_cell<T>(ad, cell.cell());
  }

  // Clean up
  storage_manager_->close_array(ad);
  err = csv_file_collection.close();
  if(err)
    return -1;

  return TILEDB_OK;
}

int Loader::load_csv(int ad, const std::string& filename) const {
  // For easy reference
  const ArraySchema* array_schema;
  // TODO: Fix error message
  int err = storage_manager_->get_array_schema(ad, array_schema);
  if(err == -1)
    return -1;
  int attribute_num = array_schema->attribute_num();
  const std::type_info& coords_type = *(array_schema->type(attribute_num));

  if(coords_type == typeid(int))
    return load_csv<int>(ad, filename);
  else if(coords_type == typeid(int64_t))
    return load_csv<int64_t>(ad, filename);
  else if(coords_type == typeid(float))
    return load_csv<float>(ad, filename);
  else if(coords_type == typeid(double))
    return load_csv<double>(ad, filename);
  else
    assert(false); // this should not happen
  return -1;
}

template<class T>
int Loader::load_csv(int ad, const std::string& filename) const {
  // Open the csv file 
  CSVLine csv_line;
  CSVFile csv_file;
  if(!csv_file.open(filename, "r")) {
// TODO: Return error code instead. Distinguish between does not exists
// and not found.

    // Clean up
    storage_manager_->forced_close_array(ad);
    std::cerr << ERROR_MSG_HEADER
              << " Cannot open CSV file '" + filename + "'.\n";
    return TILEDB_EFILE;
  }

  // For easy reference
  const ArraySchema* array_schema;
  int err = storage_manager_->get_array_schema(ad, array_schema);
  if(err == -1)
    return -1;
  int64_t line = 0;
  ssize_t cell_size = array_schema->cell_size();
  bool var_size = (cell_size == VAR_SIZE);

  // Prepare a cell buffer
  void* cell = NULL;

  if(!var_size)
    cell = malloc(cell_size);

  while(csv_file >> csv_line) {
    ++line;

    // In case of variable-sized attribute cells, calculate cell size first
    if(var_size) {
      if(cell != NULL) {
        free(cell);
        cell = NULL;
      }
      cell_size = calculate_cell_size(csv_line, array_schema);

      if(cell_size != -1)
        cell = malloc(cell_size);
    }

    // Get a logical cell
    if(cell_size == -1 || 
       !csv_line_to_cell<T>(array_schema, csv_line, cell, cell_size)) { 
      // Clean up
      storage_manager_->forced_close_array(ad); 
      csv_file.close();
      if(cell != NULL) 
        free(cell);
      // Error message
      std::stringstream ss;
      ss << "Cannot load cell from line " << line 
         << " of file '" << filename << "'.\n";
      std::cerr << ERROR_MSG_HEADER << ss.str();
      return TILEDB_EFILE;
    }

    // Write the cell in the array
    storage_manager_->write_cell<T>(ad, cell);
  }

  // Clean up 
  csv_file.close();
  storage_manager_->close_array(ad);
  free(cell);

  return TILEDB_OK;
}

int Loader::load_sorted_bin(const std::string& filename, int ad) const {
  // For easy reference
  const ArraySchema* array_schema;
  int err = storage_manager_->get_array_schema(ad, array_schema);
  if(err == -1)
    return -1;
  int attribute_num = array_schema->attribute_num();
  const std::type_info& coords_type = *(array_schema->type(attribute_num));

  if(coords_type == typeid(int))
    return load_sorted_bin<int>(filename, ad);
  else if(coords_type == typeid(int64_t))
    return load_sorted_bin<int64_t>(filename, ad);
  else if(coords_type == typeid(float))
    return load_sorted_bin<float>(filename, ad);
  else if(coords_type == typeid(double))
    return load_sorted_bin<double>(filename, ad);
  else
    assert(false); // this should not happen
  return -1;
}

template<class T>
int Loader::load_sorted_bin(const std::string& filename, int ad) const {
  // For easy reference
  const ArraySchema* array_schema;
  int err = storage_manager_->get_array_schema(ad, array_schema);
  if(err == -1)
    return -1;
  ssize_t cell_size = array_schema->cell_size();
  bool var_size = (cell_size == VAR_SIZE);

  // Open the BIN file 
  BINFile bin_file(array_schema);
  if(bin_file.open(filename, "r") == -1) {
    storage_manager_->forced_close_array(ad);
    return -1;
  }

  // Prepare a cell
  Cell cell(array_schema);

  while(bin_file >> cell) 
    storage_manager_->write_cell_sorted<T>(ad, cell.cell());

  // Clean up 
  bin_file.close();
  storage_manager_->close_array(ad);

  return TILEDB_OK;
}

inline
void Loader::set_workspace(const std::string& path) {
  workspace_ = absolute_path(path);
  assert(is_dir(workspace_));

  std::stringstream ss;
  ss << workspace_;
  if(!ends_with(workspace_, "/"))
      ss << "/";
  ss << "Loader";
  workspace_  = ss.str();
  return;
}
