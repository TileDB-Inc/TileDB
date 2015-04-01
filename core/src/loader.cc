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
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <iostream>

/******************************************************
********************* CONSTRUCTORS ********************
******************************************************/

Loader::Loader(const std::string& workspace, StorageManager* storage_manager) 
    : storage_manager_(storage_manager) {
  set_workspace(workspace);
  create_workspace(); 
}

/******************************************************
******************* LOADING FUNCTIONS *****************
******************************************************/


void Loader::load(const std::string& filename, 
                  const std::string& array_name, 
                  const std::string& fragment_name) const {
  // Load array schema
  const ArraySchema* array_schema =
      storage_manager_->load_array_schema(array_name);

  load(filename, array_schema, fragment_name);
}

void Loader::load(const std::string& filename, 
                  const ArraySchema* array_schema, 
                  const std::string& fragment_name) const {
  // For easy reference
  const std::string& array_name = array_schema->array_name();

  // Open array in CREATE mode
  const StorageManager::FragmentDescriptor* fd = 
      storage_manager_->open_fragment(array_schema, 
                                      fragment_name, 
                                      StorageManager::CREATE);

  // Prepare filenames
  std::string to_be_sorted_filename = filename;
  if(to_be_sorted_filename[0] == '~') 
    to_be_sorted_filename = std::string(getenv("HOME")) +
        to_be_sorted_filename.substr(1, filename.size()-1);
  assert(check_on_load(to_be_sorted_filename));
  std::string sorted_filename = workspace_ + "/sorted_" +
                                array_name + "_" + fragment_name + ".csv";
  std::string injected_filename("");
  
  // For easy reference
  bool regular = array_schema->has_regular_tiles();
  ArraySchema::CellOrder cell_order = array_schema->cell_order();
  
  // Inject ids if needed
  if(regular || cell_order == ArraySchema::CO_HILBERT) {
    injected_filename = workspace_ + "/injected_" +
                        array_name + "_" + fragment_name + ".csv";
    try {
      inject_ids_to_csv_file(filename, injected_filename, *array_schema); 
    } catch(LoaderException& le) {
      remove(injected_filename.c_str());
      storage_manager_->delete_fragment(array_name, fragment_name);
      throw LoaderException("Cannot load CSV file '" + filename + 
                            "'.\n " + le.what());
    }
    to_be_sorted_filename = injected_filename;
  }

  // Sort CSV
  sort_csv_file(to_be_sorted_filename, sorted_filename, *array_schema);
  remove(injected_filename.c_str());

  // Make tiles
  try {
    if(regular)
      make_tiles_regular(sorted_filename, fd);
    else
      make_tiles_irregular(sorted_filename, fd);
  } catch(LoaderException& le) {
    remove(sorted_filename.c_str());
    storage_manager_->delete_fragment(array_name, fragment_name); 
    throw LoaderException("Cannot load CSV file '" + filename + 
                          "'. " + le.what());
  } 

  // Clean up and close array 
  remove(sorted_filename.c_str());
  storage_manager_->close_fragment(fd); 
  delete array_schema;
}

void Loader::write(
    StorageManager::FragmentDescriptor* fd,
    const void* coords, size_t coords_size,
    const void* attrs, size_t attrs_size) const {
  // For easy reference
  const ArraySchema* array_schema = fd->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  size_t coords_cell_size = array_schema->cell_size(attribute_num);
  size_t attrs_cell_size = 0;
  for(int i=0; i<attribute_num; ++i)
    attrs_cell_size += array_schema->cell_size(i);

  // Number of cells to be written
  int64_t cell_num = coords_size / coords_cell_size;

  // Checks on cell num
  assert(coords_size % coords_cell_size == 0);
  assert(attrs_size % attrs_cell_size == 0);
  assert(cell_num == attrs_size / attrs_cell_size);

  // Prepare the storage manager about the pending batch of writes.
  storage_manager_->prepare_to_write(fd, coords_size + attrs_size);

  // Write each logical cell to the array
  // For the sake of performance, we distinguish all the possible
  // cobinations of regular/irregular tiles, tile/cell orders, and 
  // coordinate types. This way we perform these checks once for
  // the entire input batch of writes, rather than on a cell by
  // cell basis.
  if(array_schema->has_irregular_tiles()) { // Irregular tiles
    if(array_schema->cell_order() == ArraySchema::CO_ROW_MAJOR ||
       array_schema->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
      StorageManager::CoordsAttrs cell;
      cell.dim_num_ = dim_num;
      for(int64_t i=0; i<cell_num; ++i) {
        cell.coords_ = static_cast<const char*>(coords) + i*coords_cell_size; 
        cell.attrs_ = static_cast<const char*>(attrs) + i*attrs_cell_size; 
        storage_manager_->write_cell(fd, cell); 
      }
    } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT
      StorageManager::IdCoordsAttrs cell;
      cell.dim_num_ = dim_num;
      for(int64_t i=0; i<cell_num; ++i) {
        cell.coords_ = static_cast<const char*>(coords) + i*coords_cell_size; 
        cell.attrs_ = static_cast<const char*>(attrs) + i*attrs_cell_size; 
        cell.id_ = array_schema->cell_id_hilbert(cell.coords_);
        storage_manager_->write_cell(fd, cell); 
      }
    }
  } else { // Regular tiles
    if(array_schema->tile_order() == ArraySchema::TO_ROW_MAJOR) { 
      if(array_schema->cell_order() == ArraySchema::CO_ROW_MAJOR ||
         array_schema->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
        StorageManager::IdCoordsAttrs cell;
        cell.dim_num_ = dim_num;
        for(int64_t i=0; i<cell_num; ++i) {
          cell.coords_ = static_cast<const char*>(coords) + i*coords_cell_size; 
          cell.attrs_ = static_cast<const char*>(attrs) + i*attrs_cell_size; 
          cell.id_ = array_schema->tile_id_row_major(cell.coords_);
          storage_manager_->write_cell(fd, cell); 
        }
      } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT) {
        StorageManager::IdIdCoordsAttrs cell;
        cell.dim_num_ = dim_num;
        for(int64_t i=0; i<cell_num; ++i) {
          cell.coords_ = static_cast<const char*>(coords) + i*coords_cell_size; 
          cell.attrs_ = static_cast<const char*>(attrs) + i*attrs_cell_size; 
          cell.id_1_ = array_schema->tile_id_row_major(cell.coords_);
          cell.id_2_ = array_schema->cell_id_hilbert(cell.coords_);
          storage_manager_->write_cell(fd, cell); 
        }
      }
    } else if(array_schema->tile_order() == ArraySchema::TO_COLUMN_MAJOR) { 
      if(array_schema->cell_order() == ArraySchema::CO_ROW_MAJOR ||
         array_schema->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
        StorageManager::IdCoordsAttrs cell;
        cell.dim_num_ = dim_num;
        for(int64_t i=0; i<cell_num; ++i) {
          cell.coords_ = static_cast<const char*>(coords) + i*coords_cell_size; 
          cell.attrs_ = static_cast<const char*>(attrs) + i*attrs_cell_size; 
          cell.id_ = array_schema->tile_id_column_major(cell.coords_);
          storage_manager_->write_cell(fd, cell); 
        }
      } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT) {
        StorageManager::IdIdCoordsAttrs cell;
        cell.dim_num_ = dim_num;
        for(int64_t i=0; i<cell_num; ++i) {
          cell.coords_ = static_cast<const char*>(coords) + i*coords_cell_size; 
          cell.attrs_ = static_cast<const char*>(attrs) + i*attrs_cell_size; 
          cell.id_1_ = array_schema->tile_id_column_major(cell.coords_);
          cell.id_2_ = array_schema->cell_id_hilbert(cell.coords_);
          storage_manager_->write_cell(fd, cell); 
        }
      }
    } else if(array_schema->tile_order() == ArraySchema::TO_HILBERT) { 
      if(array_schema->cell_order() == ArraySchema::CO_ROW_MAJOR ||
         array_schema->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
        StorageManager::IdCoordsAttrs cell;
        cell.dim_num_ = dim_num;
        for(int64_t i=0; i<cell_num; ++i) {
          cell.coords_ = static_cast<const char*>(coords) + i*coords_cell_size; 
          cell.attrs_ = static_cast<const char*>(attrs) + i*attrs_cell_size; 
          cell.id_ = array_schema->tile_id_hilbert(cell.coords_);
          storage_manager_->write_cell(fd, cell); 
        }
      } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT) {
        StorageManager::IdIdCoordsAttrs cell;
        cell.dim_num_ = dim_num;
        for(int64_t i=0; i<cell_num; ++i) {
          cell.coords_ = static_cast<const char*>(coords) + i*coords_cell_size; 
          cell.attrs_ = static_cast<const char*>(attrs) + i*attrs_cell_size; 
          cell.id_1_ = array_schema->tile_id_hilbert(cell.coords_);
          cell.id_2_ = array_schema->cell_id_hilbert(cell.coords_);
          storage_manager_->write_cell(fd, cell); 
        }
      }
    } 
  }

  fd->add_buffer_to_be_freed(coords);
  fd->add_buffer_to_be_freed(attrs);
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

bool Loader::append_attribute(CSVLine* csv_line, Tile* tile) const {
  assert(tile->tile_type() == Tile::ATTRIBUTE);

  const std::type_info* cell_type = tile->cell_type();

  if(*cell_type == typeid(char)) 
    return append_attribute<char>(csv_line, tile);
  else if(*cell_type == typeid(int)) 
    return append_attribute<int>(csv_line, tile);
  else if(*cell_type == typeid(int64_t)) 
    return append_attribute<int64_t>(csv_line, tile);
  else if(*cell_type == typeid(float)) 
    return append_attribute<float>(csv_line, tile);
  else if(*cell_type == typeid(double)) 
    return append_attribute<double>(csv_line, tile);
}

template<class T>
bool Loader::append_attribute(CSVLine* csv_line, Tile* tile) const {
  assert(tile->tile_type() == Tile::ATTRIBUTE);


  T v;
  if(!(*csv_line >> v))
    return false;
  *tile << v;
 
  return true; 
}

inline
void Loader::append_cell(const ArraySchema& array_schema, 
                         CSVLine* csv_line, Tile** tiles) const {
  // For easy reference
  int attribute_num = array_schema.attribute_num();

  // Append coordinates first
  if(!append_coordinates(csv_line, tiles[attribute_num]))
    throw LoaderException("Cannot read coordinates from CSV file.");
  // Append attribute values
  for(int i=0; i<attribute_num; ++i)
    if(!(append_attribute(csv_line, tiles[i])))
      throw LoaderException("Cannot read attribute value from CSV file.");
}

bool Loader::append_coordinates(CSVLine* csv_line, Tile* tile) const {
  assert(tile->tile_type() == Tile::COORDINATE);

  const std::type_info* cell_type = tile->cell_type();

  if(*cell_type == typeid(int)) 
    return append_coordinates<int>(csv_line, tile);
  else if(*cell_type == typeid(int64_t)) 
    return append_coordinates<int64_t>(csv_line, tile);
  else if(*cell_type == typeid(float)) 
    return append_coordinates<float>(csv_line, tile);
  else if(*cell_type == typeid(double)) 
    return append_coordinates<double>(csv_line, tile);
}

template<class T>
bool Loader::append_coordinates(CSVLine* csv_line, Tile* tile) const {
  assert(tile->tile_type() == Tile::COORDINATE);
  int dim_num = tile->dim_num();

  T* coords = new T[dim_num];

  for(int i=0; i<dim_num; ++i) {
    if(!(*csv_line >> coords[i])) {
      delete [] coords;
      return false;
    }
  }

  *tile << coords;

  delete [] coords;

  return true;
}

bool Loader::check_on_load(const std::string& filename) const {
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1)
    return false; 
   
  close(fd);

  return true;
}

void Loader::create_workspace() const {
  struct stat st;
  stat(workspace_.c_str(), &st);

  // If the workspace does not exist, create it
  if(!S_ISDIR(st.st_mode)) { 
    int dir_flag = mkdir(workspace_.c_str(), S_IRWXU);
    assert(dir_flag == 0);
  }
}

void Loader::inject_ids_to_csv_file(const std::string& filename,
                                    const std::string& injected_filename,
                                    const ArraySchema& array_schema) const {
  // For easy reference
  int attribute_num = array_schema.attribute_num();
  const std::type_info* type = array_schema.type(attribute_num);

  if(*type == typeid(int)) {
    inject_ids_to_csv_file<int>(filename, injected_filename, array_schema);
  } else if(*type == typeid(int64_t)) {
    inject_ids_to_csv_file<int64_t>(filename, injected_filename, array_schema);
  } else if(*type == typeid(float)) {
    inject_ids_to_csv_file<float>(filename, injected_filename, array_schema);
  } else if(*type == typeid(double)) {
    inject_ids_to_csv_file<double>(filename, injected_filename, array_schema);
  } else {
    assert(0);
  }
}

template<class T>
void Loader::inject_ids_to_csv_file(const std::string& filename,
                                    const std::string& injected_filename,
                                    const ArraySchema& array_schema) const {
  assert(array_schema.has_regular_tiles() || 
         array_schema.cell_order() == ArraySchema::CO_HILBERT);

  // For easy reference
  int dim_num = array_schema.dim_num();
  ArraySchema::TileOrder tile_order = array_schema.tile_order();
  ArraySchema::CellOrder cell_order = array_schema.cell_order();

  // Initialization
  CSVFile csv_file_in(filename, CSVFile::READ);
  CSVFile csv_file_out(injected_filename, CSVFile::WRITE);
  CSVLine line_in, line_out;
  T* coords = new T[dim_num];

  while(csv_file_in >> line_in) {
    // Retrieve coordinates from the input line
    for(int i=0; i<dim_num; ++i) {
      if(!(line_in >> coords[i]))
        throw LoaderException("Cannot read coordinate value from CSV file."); 
    }

    // Append tile id to the new line (only for regular tiles)
    if(array_schema.has_regular_tiles()) { 
      if(tile_order == ArraySchema::TO_HILBERT)
        line_out << array_schema.tile_id_hilbert(coords);
      else if(tile_order == ArraySchema::TO_ROW_MAJOR)
        line_out << array_schema.tile_id_row_major(coords);
      else if(tile_order == ArraySchema::TO_COLUMN_MAJOR)
        line_out << array_schema.tile_id_column_major(coords);
    } 

    // Append Hilbert id to the new line (only for Hilbert cell order)
    if(cell_order == ArraySchema::CO_HILBERT)
        line_out << array_schema.cell_id_hilbert(coords);
   
    // Append the input line to the output line, 
    // and then into the output CSV file
    line_out << line_in;
    csv_file_out << line_out;
    line_out.clear(); 
  }

  delete [] coords;
}

void Loader::make_tiles_irregular(
    const std::string& filename,
    const StorageManager::FragmentDescriptor* fd) const {
  // For easy reference
  const ArraySchema& array_schema = *fd->array_schema();
  ArraySchema::CellOrder cell_order = array_schema.cell_order();
  int64_t capacity = array_schema.capacity();
 
  // Initialization 
  CSVFile csv_file(filename, CSVFile::READ);
  CSVLine csv_line;
  Tile** tiles = new Tile*[array_schema.attribute_num() + 1]; 
  int64_t tile_id = 0;
  int64_t cell_id;
  int64_t cell_num = 0;

  new_tiles(array_schema, tile_id, tiles);

  while(csv_file >> csv_line) {
    // Store tiles
    if(cell_num == capacity) {
      store_tiles(fd, tiles);
      new_tiles(array_schema, ++tile_id, tiles);
      cell_num = 0;
    }

    // Skip the Hilbert id
    if(cell_order == ArraySchema::CO_HILBERT)
      if(!(csv_line >> cell_id)) {
        delete [] tiles;
        throw LoaderException("Cannot read cell id."); 
      } 
   
    try {
      // Every CSV line is a logical cell
      append_cell(array_schema, &csv_line, tiles);     
    } catch(LoaderException& le) {
      delete [] tiles;
      throw LoaderException(le.what()); 
    }
    ++cell_num;
  }

  // Store the lastly created tiles
  store_tiles(fd, tiles);

  delete [] tiles; 
}

void Loader::make_tiles_regular(
    const std::string& filename, 
    const StorageManager::FragmentDescriptor* fd) const {
  // For easy reference
  const ArraySchema& array_schema = *fd->array_schema();
  ArraySchema::CellOrder cell_order = array_schema.cell_order();

  // Initialization
  CSVFile csv_file(filename, CSVFile::READ);
  CSVLine csv_line;
  Tile** tiles = new Tile*[array_schema.attribute_num() + 1]; 
  int64_t previous_tile_id = LD_INVALID_TILE_ID, tile_id;
  int64_t cell_id;
  
  // Process the following lines 
  while(csv_file >> csv_line) {
    if(!(csv_line >> tile_id)) {
      delete [] tiles;
      throw LoaderException("Cannot read tile id."); 
    }
    if(tile_id != previous_tile_id) {
      // Send the tiles to the storage manager and initialize new ones
      if(previous_tile_id != LD_INVALID_TILE_ID) 
        store_tiles(fd, tiles);
      new_tiles(array_schema, tile_id, tiles);
      previous_tile_id = tile_id;
    }

    // Skip the Hilbert id
    if(cell_order == ArraySchema::CO_HILBERT)
      if(!(csv_line >> cell_id)) {
        delete [] tiles;
        throw LoaderException("Cannot read cell id."); 
    }

    try{
      // Every CSV line is a logical cell
      append_cell(array_schema, &csv_line, tiles);
    } catch(LoaderException& le) {
      delete [] tiles;
      throw LoaderException(le.what()) ; 
    }
  }

  // Store the lastly created tiles
  if(previous_tile_id != LD_INVALID_TILE_ID)
    store_tiles(fd, tiles);

  delete [] tiles; 
}

inline
void Loader::new_tiles(const ArraySchema& array_schema, 
                       int64_t tile_id, Tile** tiles) const {
  // For easy reference
  int attribute_num = array_schema.attribute_num();
  int64_t capacity = array_schema.capacity();

  for(int i=0; i<=attribute_num; ++i)
    tiles[i] = storage_manager_->new_tile(&array_schema, i, tile_id, capacity);
}

bool Loader::path_exists(const std::string& path) const {
  struct stat st;
  stat(path.c_str(), &st);
  return S_ISDIR(st.st_mode);
}

inline
void Loader::set_workspace(const std::string& path) {
  workspace_ = path;
  
  // Replace '~' with the absolute path
  if(workspace_[0] == '~') {
    workspace_ = std::string(getenv("HOME")) +
                 workspace_.substr(1, workspace_.size()-1);
  }

  // Check if the input path is an existing directory 
  assert(path_exists(workspace_));
 
  workspace_ += "/Loader";
}

void Loader::sort_csv_file(const std::string& to_be_sorted_filename, 
                           const std::string& sorted_filename,    
                           const ArraySchema& array_schema) const {
  // Prepare Linux sort command
  char sub_cmd[50];
  std::string cmd;

  cmd = "sort -t, ";
  
  // For easy reference
  int dim_num = array_schema.dim_num();
  bool regular = array_schema.has_regular_tiles();
  ArraySchema::CellOrder cell_order = array_schema.cell_order();

  int c = 1;

  // First sort on the first column (tile_id)
  if(regular) { 
    cmd += "-k1,1n ";
    ++c;
  }

  if(cell_order == ArraySchema::CO_HILBERT) { 
    sprintf(sub_cmd, "-k%u,%un ", c, c); 
    cmd += sub_cmd;
    ++c;
  }

  for(int i=c; i<dim_num+c; ++i) {
    if(cell_order == ArraySchema::CO_ROW_MAJOR ||
       cell_order == ArraySchema::CO_HILBERT)
      sprintf(sub_cmd, "-k%u,%un ", i, i);
    else if (cell_order == ArraySchema::CO_COLUMN_MAJOR) 
      sprintf(sub_cmd, "-k%u,%un ", dim_num+c-i, dim_num+c-i);
    else // Unsupported order
      assert(0);
    cmd += sub_cmd;
  }

  cmd += to_be_sorted_filename + " > " + sorted_filename;

  // Invoke Linux sort command
  system(cmd.c_str());
}

inline
void Loader::store_tiles(const StorageManager::FragmentDescriptor* fd,
                         Tile** tiles) const {
  // For easy reference
  int attribute_num = fd->array_schema()->attribute_num();

  // Append attribute tiles
  for(int i=0; i<=attribute_num; ++i)
    storage_manager_->append_tile(tiles[i], fd, i);
}

