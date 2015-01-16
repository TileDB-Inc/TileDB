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

Loader::Loader(const std::string& workspace, StorageManager& storage_manager) 
    : storage_manager_(storage_manager) {
  set_workspace(workspace);
  create_workspace(); 
}

/******************************************************
******************* LOADING FUNCTIONS *****************
******************************************************/

void Loader::load(const std::string& filename, 
                  const ArraySchema& array_schema) const {
  // Open array in CREATE mode
  StorageManager::ArrayDescriptor* ad = 
      storage_manager_.open_array(array_schema);

  // Prepare filenames
  std::string to_be_sorted_filename = filename;
  if(to_be_sorted_filename[0] == '~') 
    to_be_sorted_filename = std::string(getenv("HOME")) +
        to_be_sorted_filename.substr(1, workspace_.size()-1);
  assert(check_on_load(to_be_sorted_filename));
  std::string sorted_filename = workspace_ + "/sorted_" +
                                array_schema.array_name() + ".csv";
  std::string injected_filename("");
  
  // For easy reference
  bool regular = array_schema.has_regular_tiles();
  ArraySchema::Order order = array_schema.order();
  
  // Inject ids if needed
  if(regular || order == ArraySchema::HILBERT) {
    injected_filename = workspace_ + "/injected_" +
                        array_schema.array_name() + ".csv";
    try {
      inject_ids_to_csv_file(filename, injected_filename, array_schema); 
    } catch(LoaderException& le) {
      remove(injected_filename.c_str());
      storage_manager_.delete_array(array_schema.array_name());
      throw LoaderException("[Loader] Cannot load CSV file '" + filename + 
                            "'.\n " + le.what());
    }
    to_be_sorted_filename = injected_filename;
  }

  // Sort CSV
  sort_csv_file(to_be_sorted_filename, sorted_filename, array_schema);
  remove(injected_filename.c_str());

  // Make tiles
  try {
    if(regular)
      make_tiles_regular(sorted_filename, ad, array_schema);
    else
      make_tiles_irregular(sorted_filename, ad, array_schema);
  } catch(LoaderException& le) {
    remove(sorted_filename.c_str());
    storage_manager_.delete_array(array_schema.array_name());
    throw LoaderException("[Loader] Cannot load CSV file '" + filename + 
                          "'.\n " + le.what());
  } 

  // Clean up and close array 
  remove(sorted_filename.c_str());
  storage_manager_.close_array(ad);
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

inline
void Loader::append_cell(const ArraySchema& array_schema, 
                         CSVLine* csv_line, Tile** tiles) const {
  // For easy reference
  unsigned int attribute_num = array_schema.attribute_num();

  // Append coordinates first
  if(!(*csv_line >> *tiles[attribute_num]))
    throw LoaderException("[Append cell] Cannot read coordinates "
                          "from CSV file.");
  // Append attribute values
  for(unsigned int i=0; i<attribute_num; i++)
    if(!(*csv_line >> *tiles[i]))
      throw LoaderException("[Append cell] Cannot read attribute "
                            "value from CSV file.");
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
  assert(array_schema.has_regular_tiles() || 
         array_schema.order() == ArraySchema::HILBERT);

  // For easy reference
  unsigned int dim_num = array_schema.dim_num();
  ArraySchema::Order order = array_schema.order();

  // Initialization
  CSVFile csv_file_in(filename, CSVFile::READ);
  CSVFile csv_file_out(injected_filename, CSVFile::WRITE);
  CSVLine line_in, line_out;
  std::vector<double> coordinates;
  coordinates.resize(dim_num);
  double coordinate;

  while(csv_file_in >> line_in) {
    // Retrieve coordinates from the input line
    for(unsigned int i=0; i<dim_num; i++) {
      if(!(line_in >> coordinate))
        throw LoaderException("[Inject ids] Cannot read coordinate "
                              "value from CSV file."); 
      coordinates[i] = coordinate;
    }
    // Put the id at the beginning of the output line
    if(array_schema.has_regular_tiles()) { // Regular tiles
      if(order == ArraySchema::HILBERT)
        line_out = array_schema.tile_id_hilbert(coordinates);
      else if(order == ArraySchema::ROW_MAJOR)
        line_out = array_schema.tile_id_row_major(coordinates);
      else if(order == ArraySchema::COLUMN_MAJOR)
        line_out = array_schema.tile_id_column_major(coordinates);
    } else { // Irregular tiles + Hilbert cell order
        line_out = array_schema.cell_id_hilbert(coordinates);
    }
    
    // Append the input line to the output line, 
    // and then into the output CSV file
    line_out << line_in;
    csv_file_out << line_out;
  }
}

void Loader::make_tiles_irregular(const std::string& filename,
                                  const StorageManager::ArrayDescriptor* ad, 
                                  const ArraySchema& array_schema) const {
  // For easy reference
  ArraySchema::Order order = array_schema.order();
  uint64_t capacity = array_schema.capacity();
 
  // Initialization 
  CSVFile csv_file(filename, CSVFile::READ);
  CSVLine csv_line;
  Tile** tiles = new Tile*[array_schema.attribute_num() + 1]; 
  uint64_t tile_id = 0;
  uint64_t cell_id;
  uint64_t cell_num = 0;

  new_tiles(array_schema, tile_id, tiles);

  while(csv_file >> csv_line) {
    if(cell_num == capacity) {
      store_tiles(ad, tiles);
      new_tiles(array_schema, ++tile_id, tiles);
      cell_num = 0;
    }
    // Skip the Hilbert id
    if(order == ArraySchema::HILBERT)
      if(!(csv_line >> cell_id)) {
        delete [] tiles;
        throw LoaderException("[Make tiles] Cannot read cell id."); 
      } 
   
    try {
      // Every CSV line is a logical cell
      append_cell(array_schema, &csv_line, tiles);     
    } catch(LoaderException& le) {
      delete [] tiles;
      throw LoaderException("[Make tiles] " + le.what()); 
    }
    cell_num++;
  }

  // Store the lastly created tiles
  store_tiles(ad, tiles);

  delete [] tiles; 
}

// Note: The array must be open in CREATE mode
void Loader::make_tiles_regular(const std::string& filename, 
                                const StorageManager::ArrayDescriptor* ad, 
                                const ArraySchema& array_schema) const {
  CSVFile csv_file(filename, CSVFile::READ);
  CSVLine csv_line;
  Tile** tiles = new Tile*[array_schema.attribute_num() + 1]; 
  uint64_t previous_tile_id = LD_INVALID_TILE_ID, tile_id;
  
  // Proecess the following lines 
  while(csv_file >> csv_line) {
    if(!(csv_line >> tile_id)) {
      delete [] tiles;
      throw LoaderException("[Make tiles] Cannot read tile id."); 
    }
    if(tile_id != previous_tile_id) {
      // Send the tiles to the storage manager and initialize new ones
      if(previous_tile_id != LD_INVALID_TILE_ID) 
        store_tiles(ad, tiles);
      new_tiles(array_schema, tile_id, tiles);
      previous_tile_id = tile_id;
    }

    try{
      // Every CSV line is a logical cell
      append_cell(array_schema, &csv_line, tiles);
    } catch(LoaderException& le) {
      delete [] tiles;
      throw LoaderException("[Make tiles] " + le.what()) ; 
    }
  }

  // Store the lastly created tiles
  if(previous_tile_id != LD_INVALID_TILE_ID)
    store_tiles(ad, tiles);

  delete [] tiles; 
}

inline
void Loader::new_tiles(const ArraySchema& array_schema, 
                       uint64_t tile_id, Tile** tiles) const {
  // For easy reference
  unsigned int attribute_num = array_schema.attribute_num();
  uint64_t capacity = array_schema.capacity();

  for(unsigned int i=0; i<=attribute_num; i++)
    tiles[i] = storage_manager_.new_tile(array_schema, i, tile_id, capacity);
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
  unsigned int dim_num = array_schema.dim_num();
  bool regular = array_schema.has_regular_tiles();
  ArraySchema::Order order = array_schema.order();

  if(regular || order == ArraySchema::HILBERT) { 
    // to_be_sorted_filename line format:  
    // [tile_id or hilbert_cell_id],dim#1,dim#2,...,attr#1,attr#2,...
    // The tile order is determined by tile_id for regular tiles,
    // or hilbert_cell_id for irregular tiles.
    // Ties are broken by default by sorting according to ROW_MAJOR.
    cmd += "-k1,1n ";
    for(unsigned int i=2; i<dim_num+2; i++) {
      sprintf(sub_cmd, "-k%u,%un ", i, i);
      cmd += sub_cmd;
    }
  } else { // Irregular tiles + [ROW_MAJOR or COLUMN_MAJOR]
    // to_be_sorted_filename line format:  
    // dim#1,dim#2,...,attr#1,attr#2,...
    for(unsigned int i=1; i<dim_num+1; i++) {
      if(order == ArraySchema::ROW_MAJOR)
        sprintf(sub_cmd, "-k%u,%un ", i, i);
      else if (order == ArraySchema::COLUMN_MAJOR) 
        sprintf(sub_cmd, "-k%u,%un ", dim_num+1-i, dim_num+1-i);
      else // Unsupported order
        assert(0);
      cmd += sub_cmd;
    }
  }
  cmd += to_be_sorted_filename + " > " + sorted_filename;

  // Invoke Linux sort command
  system(cmd.c_str());
}

inline
void Loader::store_tiles(const StorageManager::ArrayDescriptor* ad,
                         Tile** tiles) const {
  // For easy reference
  const ArraySchema& array_schema = ad->array_schema(); 
  unsigned int attribute_num = array_schema.attribute_num();

  // Append attribute tiles
  for(unsigned int i=0; i<=attribute_num; i++)
    storage_manager_.append_tile(tiles[i], ad, i);
}

