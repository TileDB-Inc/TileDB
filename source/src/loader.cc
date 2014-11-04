/**
 * @file   loader.cc
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

Loader::Loader(const std::string& workspace,
               StorageManager& storage_manager,
               uint64_t tile_size) 
    : storage_manager_(storage_manager), tile_size_(tile_size) {
  set_workspace(workspace);
  create_workspace(); 
}

/******************************************************
******************* LOADING FUNCTIONS *****************
******************************************************/

void Loader::load(const std::string& filename, 
                  const ArraySchema& array_schema,
                  Loader::Order order) const {
  try {
    storage_manager_.open_array(array_schema.array_name(),  
                               StorageManager::CREATE);

    std::string to_be_sorted_filename = filename;
    if(to_be_sorted_filename[0] == '~') 
      to_be_sorted_filename = std::string(getenv("HOME")) +
          to_be_sorted_filename.substr(1, workspace_.size()-1);

    // Check filename (to be sorted)
    check_on_load(to_be_sorted_filename);
    std::string sorted_filename = workspace_ + "/sorted_" +
                                  array_schema.array_name() + ".csv";
    bool regular = array_schema.has_regular_tiles();
    std::string injected_filename;
  
    // Inject ids if needed
    if(regular || order == HILBERT) {
      injected_filename = workspace_ + "/injected_" +
                                      array_schema.array_name() + ".csv";
      inject_ids_to_csv_file(filename, injected_filename, 
                             array_schema, order); 
      to_be_sorted_filename = injected_filename;
    }

    // Sort CSV
    sort_csv_file(to_be_sorted_filename, sorted_filename, array_schema, order);
    remove(injected_filename.c_str());

    // Make tiles
    if(regular)
      make_tiles_regular(sorted_filename, array_schema);
    else
      make_tiles_irregular(sorted_filename, array_schema, order);
 
    remove(sorted_filename.c_str());
    storage_manager_.close_array(array_schema.array_name());

  } catch(CSVFileException& cfe) {
    // TODO: exception safety
    throw LoaderException("CSVFileException caught by Loader: " + cfe.what());
  } catch(TileException& te) {
    // TODO: exception safety
    throw LoaderException("TileException caught by Loader: " + te.what());
  } catch(StorageManagerException& sme) {
    // TODO: exception safety
    throw LoaderException("StorageManagerException caught by Loader: " + 
                          sme.what());
  } catch(LoaderException& le) {
    // TODO: exception safety
    std::cout << le.what() << "\n";
  }
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

template<class T>
inline
void Loader::append_attribute_value(CSVLine* csv_line, Tile* tile) const {
    T v;
    if(!((*csv_line) >> v))
      throw LoaderException("Cannot read attribute value.");  
    (*tile) << v;
} 

inline
void Loader::append_cell(const ArraySchema& array_schema, 
                         CSVLine* csv_line, Tile** tiles) const {
  unsigned int attribute_num = array_schema.attribute_num();
  unsigned int dim_num = array_schema.dim_num();

  // Append coordinates
  if(array_schema.dim_type() == ArraySchema::INT)
    append_coordinates<int>(dim_num, csv_line, tiles[attribute_num]);
  else if(array_schema.dim_type() == ArraySchema::INT64_T)
    append_coordinates<int64_t>(dim_num, csv_line, tiles[attribute_num]);
  else if(array_schema.dim_type() == ArraySchema::FLOAT)
    append_coordinates<float>(dim_num, csv_line, tiles[attribute_num]);
  else if(array_schema.dim_type() == ArraySchema::DOUBLE)
    append_coordinates<double>(dim_num, csv_line, tiles[attribute_num]);

  // Append attribute values
  for(unsigned int i=0; i<attribute_num; i++) {
    if(array_schema.attribute_type(i) == ArraySchema::INT)
      append_attribute_value<int>(csv_line, tiles[i]); 
    else if(array_schema.attribute_type(i) == ArraySchema::INT64_T)
      append_attribute_value<int64_t>(csv_line, tiles[i]); 
    else if(array_schema.attribute_type(i) == ArraySchema::FLOAT)
      append_attribute_value<float>(csv_line, tiles[i]); 
    else if(array_schema.attribute_type(i) == ArraySchema::DOUBLE)
      append_attribute_value<double>(csv_line, tiles[i]); 
  }
}

template<class T>
inline
void Loader::append_coordinates(unsigned int dim_num,
                                     CSVLine* csv_line, Tile* tile) const {
  std::vector<T> coordinates;
  T c;

  for(unsigned int i=0; i<dim_num; i++) {
    if(!((*csv_line) >> c))
      throw LoaderException("Cannot read coordinate.");  
    coordinates.push_back(c);
  }
  
  (*tile) << coordinates;
}

void Loader::check_on_load(const std::string& filename) const {
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) 
    throw LoaderException("Cannot load CSV file: File '" + 
                          filename + "' does not exist.");
  
  struct stat st;
  fstat(fd, &st);
  if(st.st_size == 0) {
    close(fd);
    throw LoaderException("Cannot load CSV file: File '" + 
                          filename + "' is empty.");
  }
  
  close(fd);
}

void Loader::create_workspace() const {
  // If the workspace does not exist, create it
  struct stat st;
  stat(workspace_.c_str(), &st);
  if(!S_ISDIR(st.st_mode)) 
    if(mkdir(workspace_.c_str(), S_IRWXU) != 0)
      throw LoaderException("Cannot create workspace.");
}

inline
void Loader::init_tiles(const ArraySchema& array_schema, 
                        uint64_t tile_id, Tile** tiles) const {
  unsigned int attribute_num = array_schema.attribute_num();
  unsigned int dim_num = array_schema.dim_num();

  // Init attribute tiles
  for(unsigned int i=0; i<attribute_num; i++) {
    if(array_schema.attribute_type(i) == ArraySchema::INT)
      tiles[i] = new AttributeTile<int>(tile_id);
    else if(array_schema.attribute_type(i) == ArraySchema::INT64_T)
      tiles[i] = new AttributeTile<int64_t>(tile_id);
    else if(array_schema.attribute_type(i) == ArraySchema::FLOAT)
      tiles[i] = new AttributeTile<float>(tile_id);
    else if(array_schema.attribute_type(i) == ArraySchema::DOUBLE)
      tiles[i] = new AttributeTile<double>(tile_id);
  }

  // Init cooordinate tile
  if(array_schema.dim_type() == ArraySchema::INT)
    tiles[attribute_num] = new CoordinateTile<int>(tile_id, dim_num);
  else if(array_schema.dim_type() == ArraySchema::INT64_T)
    tiles[attribute_num] = new CoordinateTile<int64_t>(tile_id, dim_num);
  else if(array_schema.dim_type() == ArraySchema::FLOAT)
    tiles[attribute_num] = new CoordinateTile<float>(tile_id, dim_num);
  else if(array_schema.dim_type() == ArraySchema::DOUBLE)
    tiles[attribute_num] = new CoordinateTile<double>(tile_id, dim_num);
}

void Loader::inject_ids_to_csv_file(const std::string& filename,
                                    const std::string& injected_filename,
                                    const ArraySchema& array_schema,
                                    Loader::Order order) const {
  assert(array_schema.has_regular_tiles() || order == HILBERT);

  CSVFile csv_file_in(filename, CSVFile::READ);
  CSVFile csv_file_out(injected_filename, CSVFile::WRITE);
  CSVLine line_in, line_out;
  double coordinate;

  while(csv_file_in >> line_in) {
    std::vector<double> coordinates;
    // Retrieve coordinates from the input line
    for(unsigned int i=0; i<array_schema.dim_num(); i++) {
      if(line_in >> coordinate == false)
        throw LoaderException("Invalid format for CSV file '" 
                              + filename + "'.");
      coordinates.push_back(coordinate);
    }
    // Put the id at the beginning of the output line
    if(array_schema.has_regular_tiles()) { // Regular tiles
      if(order == HILBERT)
        line_out = array_schema.tile_id_hilbert(coordinates);
      else if(order == ROW_MAJOR)
        line_out = array_schema.tile_id_row_major(coordinates);
      else if(order == COLUMN_MAJOR)
        line_out = array_schema.tile_id_column_major(coordinates);
    } else if(order == HILBERT) { // Irregular tiles + Hilbert cell order
        line_out = array_schema.cell_id_hilbert(coordinates);
    }
    
    // Append the input line to the output line, 
    // and then into the output CSV file
    line_out << line_in;
    csv_file_out << line_out;
  }
}

void Loader::make_tiles_irregular(const std::string& filename, 
                                  const ArraySchema& array_schema, 
                                  Loader::Order order) const {
  assert(storage_manager_.is_open(array_schema.array_name()));
  assert(storage_manager_.array_mode(array_schema.array_name()) == 
         StorageManager::CREATE);

  CSVFile csv_file(filename, CSVFile::READ);
  CSVLine csv_line;
  Tile** tiles = new Tile*[array_schema.attribute_num() + 1]; 
  uint64_t tile_id = 0;
  uint64_t cell_id;
  uint64_t cell_num = 0;
  uint64_t max_cell_num = LD_MAX_TILE_SIZE / array_schema.max_cell_size();

  init_tiles(array_schema, tile_id, tiles);

  // Proecess the following lines 
  while(csv_file >> csv_line) {
    if(order == HILBERT)
      csv_line >> cell_id; // Skip the hilbert id
    if(cell_num == max_cell_num) {
      // Send the tiles to the storage manager and initialize new ones
      store_tiles(array_schema, tiles);
      init_tiles(array_schema, ++tile_id, tiles);
      cell_num = 0;
    }

    append_cell(array_schema, &csv_line, tiles); // Every CSV line is a cell
    cell_num++;
  }

  // Store the lastly created tiles
  store_tiles(array_schema, tiles);

  delete [] tiles; 
}

// Note: The array must be open in CREATE mode
void Loader::make_tiles_regular(const std::string& filename, 
                                const ArraySchema& array_schema) const {
  assert(storage_manager_.is_open(array_schema.array_name()));
  assert(storage_manager_.array_mode(array_schema.array_name()) == 
         StorageManager::CREATE);

  CSVFile csv_file(filename, CSVFile::READ);
  CSVLine csv_line;
  Tile** tiles = new Tile*[array_schema.attribute_num() + 1]; 
  uint64_t previous_tile_id, tile_id;
  
  // Handle the first line, in order to properly initialize the tiles
  // for the first time
  csv_file >> csv_line;
  csv_line >> tile_id;
  init_tiles(array_schema, tile_id, tiles);
  append_cell(array_schema, &csv_line, tiles); // Every CSV line is a cell
  previous_tile_id = tile_id; 

  // Proecess the following lines 
  while(csv_file >> csv_line) {
    csv_line >> tile_id;
    if(tile_id != previous_tile_id) {
      // Send the tiles to the storage manager and initialize new ones
      store_tiles(array_schema, tiles);
      init_tiles(array_schema, tile_id, tiles);
      previous_tile_id = tile_id;
    }

    append_cell(array_schema, &csv_line, tiles); // Every CSV line is a cell
  }

  // Store the lastly created tiles
  store_tiles(array_schema, tiles);

  delete [] tiles; 
}

inline
void Loader::set_workspace(const std::string& workspace) {
  workspace_ = workspace + "/Loader";
  
  // Replace '~' with the absolute path (mkdir does not recogize '~')
  if(workspace_[0] == '~') {
    workspace_ = std::string(getenv("HOME")) +
                 workspace_.substr(1, workspace_.size()-1);
  }
}

void Loader::sort_csv_file(const std::string& to_be_sorted_filename, 
                           const std::string& sorted_filename,    
                           const ArraySchema& array_schema, 
                           Loader::Order order) const {
  // Prepare Linux sort command
  unsigned int buffer_size = LD_SORT_BUFFER_SIZE; 
  char sub_cmd[50];
  std::string cmd;

  sprintf(sub_cmd, "sort -t, -S %uG ", buffer_size);
  cmd += sub_cmd;
  unsigned int dim_num = array_schema.dim_num();

  if(array_schema.has_regular_tiles() || order == HILBERT) { 
    // to_be_sorted_filename line format:  
    // [tile_id or hilbert_cell_id],dim#1,dim#2,...,attr#1,attr#2,...
    // The tile order is determined by tile_id for regular tiles,
    // or hilbert_cell_id for irregular tiles.
    // Ties are broken by default by sorting according to ROW_MAJOR.
    cmd += "-k1,1g ";
    for(unsigned int i=2; i<dim_num+2; i++) {
      sprintf(sub_cmd, "-k%u,%ug ", i, i);
      cmd += sub_cmd;
    }
  } else { // Irregular tiles + [ROW_MAJOR or COLUMN_MAJOR]
    // to_be_sorted_filename line format:  
    // dim#1,dim#2,...,attr#1,attr#2,...
    for(unsigned int i=1; i<dim_num+1; i++) {
      if(order == ROW_MAJOR)
        sprintf(sub_cmd, "-k%u,%ug ", i, i);
      else // order == COLUMN_MAJOR
        sprintf(sub_cmd, "-k%u,%ug ", dim_num+1-i, dim_num+1-i);
      cmd += sub_cmd;
    }
  }
  cmd += to_be_sorted_filename + " > " + sorted_filename;

  // Invoke Linux sort command
  system(cmd.c_str());
}

inline
void Loader::store_tiles(const ArraySchema& array_schema, Tile** tiles) const {
  // For easy reference
  const std::string& array_name = array_schema.array_name();
  unsigned int attribute_num = array_schema.attribute_num();

  // Append attribute tiles
  for(unsigned int i=0; i<attribute_num; i++)
    storage_manager_.append_tile(tiles[i], array_name, 
                                 array_schema.attribute_name(i));
  // Append coordinate tile
  storage_manager_.append_tile(tiles[attribute_num], array_name);
}

// Explicit template instantiatons
template void Loader::append_attribute_value<int>(
    CSVLine* csv_line, Tile* tile) const;
template void Loader::append_attribute_value<int64_t>(
    CSVLine* csv_line, Tile* tile) const;
template void Loader::append_attribute_value<float>(
    CSVLine* csv_line, Tile* tile) const;
template void Loader::append_attribute_value<double>(
    CSVLine* csv_line, Tile* tile) const;
template void Loader::append_coordinates<int>(unsigned int dim_num, 
    CSVLine* csv_line, Tile* tile) const;
template void Loader::append_coordinates<int64_t>(
    unsigned int dim_num, CSVLine* csv_line, Tile* tile) const;
template void Loader::append_coordinates<float>(
    unsigned int dim_num, CSVLine* csv_line, Tile* tile) const;
template void Loader::append_coordinates<double>(
    unsigned int dim_num, CSVLine* csv_line, Tile* tile) const;
