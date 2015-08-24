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
#include "progress_bar.h"
#include "special_values.h"
#include "utils.h"
#include <assert.h>
#include <fcntl.h>
#include <iostream>
#include <sstream>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

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

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

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
  BINFileCollection<T> bin_file_collection;
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
