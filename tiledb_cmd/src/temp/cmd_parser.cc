/**
 * @file   cmd_parser.cc
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
 * This file implements the CmdParser class.
 */

#include "cmd_parser.h"
#include "utils.h"
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <algorithm>
#include <bitset>

/******************************************************
******************* PARSING METHODS *******************
******************************************************/

void CmdParser::parse_clear_array(const CommandLine& cl) const {
  // Check if correct arguments have been ginen
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if(cl.array_names_.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array"
              << " names.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ | PS_CLEAR_ARRAY_BITMAP) != PS_CLEAR_ARRAY_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant options"
              << " provided.\n";
    exit(-1);
  }
}

const ArraySchema* CmdParser::parse_define_array(const CommandLine& cl) const {
  // Mandatory arguments
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ATTRIBUTE_NAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Attribute names not"
              << " provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_DIM_NAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Dimension names not"
              << " provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_DIM_DOMAIN_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Dimension domains not"
              << " provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_TYPE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Attribute and dimension"
              << " types not provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if(cl.array_names_.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array names"
              << " provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ | PS_DEFINE_ARRAY_BITMAP) != PS_DEFINE_ARRAY_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant options"
              << " provided.\n";
    exit(-1);
  }

  // Prepare arguments for ArraySchema object creation and check soundness
  check_array_names(cl); 
  std::vector<std::string> attribute_names = check_attribute_names(cl);
  std::vector<std::string> dim_names = check_dim_names(cl, attribute_names);
  std::vector<std::pair<double, double> > dim_domains = 
      check_dim_domains(cl);
  std::vector<const std::type_info*> types = check_types(cl);
  ArraySchema::CellOrder cell_order = check_cell_order(cl);
  ArraySchema::TileOrder tile_order = check_tile_order(cl);
  int64_t capacity = check_capacity(cl);
  int consolidation_step = check_consolidation_step(cl);
  std::vector<double> tile_extents = check_tile_extents(cl, dim_domains); 
 
  // Assign default values
  if(consolidation_step == -1) 
    consolidation_step = AS_CONSOLIDATION_STEP;
  if(capacity == -1) 
    capacity = AS_CAPACITY;
  if(cell_order == ArraySchema::CO_NONE) 
    cell_order = AS_CELL_ORDER;
  if(tile_order == ArraySchema::TO_NONE)
    tile_order = AS_TILE_ORDER;
 
  // Return array schema
  if(tile_extents.size() == 0) { // Irregular tiles
    return new ArraySchema(
        cl.array_names_[0], attribute_names, dim_names, 
        dim_domains, types, cell_order, consolidation_step, capacity);
  } else { // Regular tiles 
    return new ArraySchema(
        cl.array_names_[0], attribute_names, dim_names, 
        dim_domains, types, tile_order, tile_extents, 
        consolidation_step, capacity, cell_order);
  }
}

void CmdParser::parse_delete_array(const CommandLine& cl) const {
  // Check if correct arguments have been ginen
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if(cl.array_names_.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array names"
              << " provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ | PS_DELETE_ARRAY_BITMAP) != PS_DELETE_ARRAY_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] options"
              << " provided.\n";
    exit(-1);
  }
}

void CmdParser::parse_export_to_csv(const CommandLine& cl) const {
  // Check if correct arguments have been given
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_FILENAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] CSV file name not"
              << " provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if(cl.array_names_.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array names"
              << " provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ | PS_EXPORT_TO_CSV_BITMAP) != PS_EXPORT_TO_CSV_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant options"
              << " provided.\n";
    exit(-1);
  }
}

/* TODO

void CmdParser::parse_filter(const CommandLine& cl) const {
  // Check if correct arguments have been ginen
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_EXPRESSION_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Filter expression not"
              << " provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_RESULT_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Result name not provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if(cl.array_names_.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array names"
              << " provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ | PS_FILTER_BITMAP) != PS_FILTER_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant arguments"
              << " provided.\n";
    exit(-1);
  }
}

*/

/* TODO

void CmdParser::parse_join(const CommandLine& cl) const {
  // Check if correct arguments have been ginen
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_RESULT_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Result name not provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if(cl.array_names_.size() != 2) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Exactly two input array"
                 " names must be given.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ | PS_JOIN_BITMAP) != PS_JOIN_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant arguments"
              << " provided.\n";
    exit(-1);
  }
}

*/

void CmdParser::parse_load_csv(const CommandLine& cl) const {
  // Check if correct arguments have been ginen
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_FILENAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] CSV file name not"
              << " provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if(cl.array_names_.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array names"
              << " provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ | PS_LOAD_CSV_BITMAP) != PS_LOAD_CSV_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant options"
              << " provided.\n";
    exit(-1);
  }
}

/* TODO

std::pair<std::vector<double>,int64_t> CmdParser::parse_nearest_neighbors(
    const CommandLine& cl) const {
  // Check if correct arguments have been ginen
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_NUMBER_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Number of nearest"
                 " neighbors not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_RESULT_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Result name not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_COORDINATE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Reference cell not"
              << " provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if(cl.array_names_.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array names"
              << " provided.\n";
    exit(-1);
  }
  if(cl.numbers_.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one numbers of"
                 " nearest neighbors provided.\n";
    exit(-1);
  }

  if((cl.arg_bitmap_ | PS_NN_BITMAP) != PS_NN_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant arguments"
              << " provided.\n";
    exit(-1);
  }

  // Prepare output
  std::vector<double> coords = check_coordinates(cl);
  std::vector<int64_t> N = check_numbers(cl); 

  // N[0] cannot be zero
  if(N[0] == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] The number of nearest"
                 " neighbors cannot be zero.\n";
    exit(-1);
  }

  return std::pair<std::vector<double>,int>(coords, N[0]);  
}

*/

/* TODO

void CmdParser::parse_retile(
    const CommandLine& cl,
    int64_t& capacity,
    ArraySchema::CellOrder& cell_order,
    std::vector<double>& tile_extents) const {
  // Check if correct arguments have been ginen
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if(cl.array_names_.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array names"
              << " provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ | PS_RETILE_BITMAP) != PS_RETILE_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant arguments"
              << " provided.\n";
    exit(-1);
  }

  // Check if at least one of {capacity, order, tile extents} was given
  if(cl.arg_bitmap_ == (CL_WORKSPACE_BITMAP | CL_ARRAY_NAME_BITMAP)) {
    std::cerr << "[TileDB::CmdParser::fatal_error] At least one of capacity,"
                 " cell order, or tile extents must be given.\n"; 
    exit(-1);
  }

  capacity = check_capacity(cl);
  cell_order = check_cell_order(cl);
  tile_extents = check_tile_extents(cl); 
}

*/

/* TODO

std::vector<double> CmdParser::parse_subarray(const CommandLine& cl) const {
  // Check if correct arguments have been ginen
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_RANGE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Range not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_RESULT_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Result name not provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if(cl.array_names_.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array names"
              << " provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ | PS_SUBARRAY_BITMAP) != PS_SUBARRAY_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant arguments"
              << " provided.\n";
    exit(-1);
  }

  // Prepare range
  std::vector<double> range = check_range(cl); 

  return range;  
}

*/

void CmdParser::parse_update_csv(const CommandLine& cl) const {
  // Check if correct arguments have been ginen
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_FILENAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] File name not provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if(cl.array_names_.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array names"
              << " provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ | PS_UPDATE_BITMAP) != PS_UPDATE_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant options"
              << " provided.\n";
    exit(-1);
  }
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

void CmdParser::check_array_names(const CommandLine& cl) const {
  // Check array name validity
  for(int i=0; i<cl.array_names_.size(); ++i) {
    if(!is_valid_name(cl.array_names_[i])) {
      std::cerr << "[TileDB::CmdParser::fatal_error] The array name can contain"
                << " only alphanumerics or '_'.\n";
      exit(-1);
    }
  }

  // Check for duplicate array names
  std::set<std::string> set_array_names(cl.array_names_.begin(), 
                                        cl.array_names_.end());
  if(set_array_names.size() < cl.array_names_.size()) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Duplicate array names"
              << " array names provided.\n";
    exit(-1);
  }
}

std::vector<std::string> CmdParser::check_attribute_names(
    const CommandLine& cl) const {
  std::vector<std::string> attribute_names;
  attribute_names.reserve(cl.attribute_names_.size());

  // Check attribute name validity
  for(int i=0; i<cl.attribute_names_.size(); ++i) {
    if(!is_valid_name(cl.attribute_names_[i])) {
      std::cerr << "[TileDB::CmdParser::fatal_error] The attribute name can" 
                << " contain only alphanumerics or '_'.\n";
      exit(-1);
    }
    attribute_names.push_back(cl.attribute_names_[i]);
  }

  // Check for duplicate attribute names
  std::set<std::string> set_attribute_names(attribute_names.begin(), 
                                            attribute_names.end());
  if(set_attribute_names.size() < attribute_names.size()) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Duplicate attribute names"
              << " provided.\n";
    exit(-1);
  }

  return attribute_names;
}

int64_t CmdParser::check_capacity(const CommandLine& cl) const {
  if(cl.capacity_ == NULL)
    return -1;

  if(!is_integer(cl.capacity_)) {
    std::cerr << "[TileDB::CmdParser::fatal_error] The capacity provided is"
              << " not an integer.\n"; 
    exit(-1);
  }

  int64_t capacity;
  sscanf(cl.capacity_, "%lld", &capacity); 

  // Capacity must be positive
  if(capacity <= 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] The capacity must"
              << " be positive.\n"; 
    exit(-1);
  }
  
  return capacity;
}

int CmdParser::check_consolidation_step(const CommandLine& cl) const {
  if(cl.consolidation_step_ == NULL)
    return -1;

  if(!is_integer(cl.consolidation_step_)) {
    std::cerr << "[TileDB::CmdParser::fatal_error] The consolidation step"
              << " provided is not an integer.\n";  
    exit(-1);
  }

  int consolidation_step;
  sscanf(cl.consolidation_step_, "%d", &consolidation_step); 

  // Capacity must be positive
  if(consolidation_step <= 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] The consolidation step must"
              << " be positive.\n"; 
    exit(-1);
  }

  return consolidation_step;
}

/* TODO

std::vector<double> CmdParser::check_coordinates(const CommandLine& cl) const {
  std::vector<double> coords;

  // Check if the coordinates are real numbers.
  for(int i=0; i<cl.coords_.size(); ++i) {
    if(!is_real(cl.coords_[i])) { 
      std::cerr << "[TileDB::CmdParser::fatal_error] The coordinates must be" 
                << " positive real numbers.\n";
      exit(-1);
    } else {
      double coord; 
      sscanf(cl.coords_[i], "%lf", &coord);
      coords.push_back(coord);
    }
  }

  return coords;
}

*/

std::vector<std::pair<double, double> > CmdParser::check_dim_domains(
   const CommandLine& cl) const {
  std::vector<std::pair<double, double> > dim_domains;

  // Check the number of domain bounds
  if(cl.dim_domains_.size() != 2*cl.dim_names_.size()) { 
    std::cerr << "[TileDB::CmdParser::fatal_error] The number of domain bounds"
              << " does not agree with the number of dimensions. There should"
              << " be a lower and an upper bound per dimension.\n";
    exit(-1);
  }

  // Check if the domain bounds are real numbers, and if the lower bound
  // is smaller than or equal to the upper.
  double lower, upper; 
  for(int i=0; i<cl.dim_names_.size(); ++i) {
    if(!is_real(cl.dim_domains_[2*i]) || 
       !is_real(cl.dim_domains_[2*i+1])) {
      std::cerr << "[TileDB::CmdParser::fatal_error] The domain bounds must be" 
                << " real numbers.\n";
      exit(-1);
    } else {
      sscanf(cl.dim_domains_[2*i], "%lf", &lower);
      sscanf(cl.dim_domains_[2*i+1], "%lf", &upper);
      if(lower > upper) {
        std::cerr << "[TileDB::CmdParser::fatal_error] A lower domain bound"
                  << " cannot be larger than its corresponding upper.\n";
        exit(-1);
      }
      dim_domains.push_back(std::pair<double,double>(lower,upper));
    }
  }

  return dim_domains;
}

std::vector<std::string> CmdParser::check_dim_names(
    const CommandLine& cl,
    const std::vector<std::string>& attribute_names) const {
  std::vector<std::string> dim_names;
  dim_names.reserve(cl.dim_names_.size());

  // Check dimension name validity
  for(int i=0; i<cl.dim_names_.size(); ++i) {
    if(!is_valid_name(cl.dim_names_[i])) {
      std::cerr << "[TileDB::CmdParser::fatal_error] The dimension names can" 
                << " contain only alphanumerics or '_'.\n";
      exit(-1);
    }
    dim_names.push_back(cl.dim_names_[i]);
  }

  // Check for duplicate dimension names
  std::set<std::string> set_dim_names(dim_names.begin(), 
                                      dim_names.end());
  if(set_dim_names.size() < dim_names.size()) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Duplicate dimension names"
              << " are not allowed.\n";
    exit(-1);
  }

  // Check if any dimension name is the same as an attribute name
  std::set<std::string> set_attribute_names(attribute_names.begin(), 
                                            attribute_names.end());
  std::vector<std::string> intersect; 
  std::set_intersection(set_attribute_names.begin(), 
                        set_attribute_names.end(),
                        set_dim_names.begin(),
                        set_dim_names.end(),
                        std::back_inserter(intersect));

  if(intersect.size()) {
    std::cerr << "[TileDB::CmdParser::fatal_error] A dimension cannot have the"
              << " same name as an attribute.\n";
    exit(-1);
  }

  return dim_names;
}

/* TODO

std::vector<int64_t> CmdParser::check_numbers(const CommandLine& cl) const {
  std::vector<int64_t> numbers;

  // Check if the coordinates are real numbers.
  for(int i=0; i<cl.numbers_.size(); ++i) {
    if(!is_integer(cl.numbers_[i])) { 
      std::cerr << "[TileDB::CmdParser::fatal_error] The 'number' argument must"
                   " be a positive integer.\n";
      exit(-1);
    } else {
      int64_t N; 
      sscanf(cl.numbers_[i], "%llu", &N);
      numbers.push_back(N);
    }
  }

  return numbers;
}

*/

ArraySchema::CellOrder CmdParser::check_cell_order(
    const CommandLine& cl) const {
  if(cl.cell_order_ == NULL) {
    return ArraySchema::CO_NONE;
  } else if(!strcmp(cl.cell_order_, "row-major")) {
    return ArraySchema::CO_ROW_MAJOR;
  } else if(!strcmp(cl.cell_order_, "column-major")) {
    return ArraySchema::CO_COLUMN_MAJOR;
  } else if(!strcmp(cl.cell_order_, "hilbert")) {
    return ArraySchema::CO_HILBERT;
  } else {
    std::cerr << "[TileDB::CmdParser::fatal_error] Unknown cell order.\n";  
    exit(-1);
  }
}

ArraySchema::TileOrder CmdParser::check_tile_order(
    const CommandLine& cl) const {
  // Tile order must not be given if the tiles are irregular
  if(cl.tile_extents_.size() == 0 && cl.tile_order_ != NULL) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Tile order is meaningless\n"  
              << " in the case of irregular tiles.\n";
    exit(-1);
  }

  if(cl.tile_order_ == NULL) {
    return ArraySchema::TO_NONE;
  } else if(!strcmp(cl.tile_order_, "row-major")) {
    return ArraySchema::TO_ROW_MAJOR;
  } else if(!strcmp(cl.tile_order_, "column-major")) {
    return ArraySchema::TO_COLUMN_MAJOR;
  } else if(!strcmp(cl.tile_order_, "hilbert")) {
    return ArraySchema::TO_HILBERT;
  } else {
    std::cerr << "[TileDB::CmdParser::fatal_error] Unknown tile order.\n"; 
    exit(-1);
  }
}

/* TODO

std::vector<double> CmdParser::check_range(const CommandLine& cl) const {
  std::vector<double> range;

  // Check the number of domain bounds
  if(cl.range_.size() % 2 != 0) { 
    std::cerr << "[TileDB::CmdParser::fatal_error] The number of range bounds"
              << " must be even.\n";
    exit(-1);
  }

  // Check if the range bounds are real numbers, and if the lower is bound
  // is smaller than or equal to the upper.
  int dim_num = cl.range_.size()/2;
  for(int i=0; i<dim_num; ++i) {
    if(!is_real(cl.range_[2*i]) || 
       !is_real(cl.range_[2*i+1])) {
      std::cerr << "[TileDB::CmdParser::fatal_error] The domain bounds must be" 
                << " real numbers.\n";
      exit(-1);
    } else {
      double lower, upper; 
      sscanf(cl.range_[2*i], "%lf", &lower);
      sscanf(cl.range_[2*i+1], "%lf", &upper);
      if(lower > upper) {
        std::cerr << "[TileDB::CmdParser::fatal_error] A lower domain bound"
                  << " cannot be larger than its corresponding upper.\n";
        exit(-1);
      }
      range.push_back(lower);
      range.push_back(upper);
    }
  }

  return range;
}

*/

/* TODO: remove
std::vector<double> CmdParser::check_tile_extents(const CommandLine& cl) const {
  std::vector<double> tile_extents;
 
  if(cl.tile_extents_.size() == 0)
    return tile_extents;
   
  // Check if the tile extents are real numbers
  for(int i=0; i<cl.tile_extents_.size(); ++i) {
    if(!is_real(cl.tile_extents_[i])) {
      std::cerr << "[TileDB::CmdParser::fatal_error] The tile extents must be"
                << " real numbers.\n";
      exit(-1);
    }
    double tile_extent;
    sscanf(cl.tile_extents_[i], "%lf", &tile_extent); 
    tile_extents.push_back(tile_extent);
  }

  return tile_extents;
}
*/

std::vector<double> CmdParser::check_tile_extents(
    const CommandLine& cl,
    const std::vector<std::pair<double, double> >& dim_domains) const {
  std::vector<double> tile_extents;
 
  if(cl.tile_extents_.size() == 0)
    return tile_extents;
   
  // Check if the number of tile extents is the same as the number of dimensions
  if(cl.tile_extents_.size() != cl.dim_names_.size()) {
    std::cerr << "[TileDB::CmdParser::fatal_error] The number of tile extents"
              << " must be the same as the number of dimensions.\n";
    exit(-1);
  }

  // Check if the tile extents are real numbers
  double tile_extent;
  for(int i=0; i<cl.tile_extents_.size(); ++i) {
    if(!is_real(cl.tile_extents_[i])) {
      std::cerr << "[TileDB::CmdParser::fatal_error] The tile extents must be"
                << " real numbers.\n";
      exit(-1);
    }
    sscanf(cl.tile_extents_[i], "%lf", &tile_extent); 
    tile_extents.push_back(tile_extent);
  }

  // Check if each tile extent is smaller than the corresponding domain range
  for(int i=0; i<tile_extents.size(); ++i) {
    if(tile_extents[i] > dim_domains[i].second - dim_domains[i].first + 1) {
      std::cerr << "[TileDB::CmdParser::fatal_error] The tile extents must not"
                << " exceed their corresponding domain ranges.\n";
      exit(-1);
    }
  }

  return tile_extents;
}

std::vector<const std::type_info*> CmdParser::check_types(
    const CommandLine& cl) const {
  std::vector<const std::type_info*> types;

  // Check number of types
  if(cl.types_.size() != cl.attribute_names_.size() + 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] The number of types should"
              << " be equal to the number of attributes plus 1 (the last"  
              << " corresponds to the coordinates).\n";
    exit(-1);
  }

  // Check soundness of attribute types
  for(int i=0; i<cl.types_.size()-1; ++i) {
    if(!strcmp(cl.types_[i], "char")) {
      types.push_back(&typeid(char));
    } else if(!strcmp(cl.types_[i], "int")) {
      types.push_back(&typeid(int));
    } else if(!strcmp(cl.types_[i], "int64_t")) {
      types.push_back(&typeid(int64_t));
    } else if(!strcmp(cl.types_[i], "float")) {
      types.push_back(&typeid(float));
    } else if(!strcmp(cl.types_[i], "double")) {
      types.push_back(&typeid(double));
    } else {
      std::cerr << "[TileDB::CmdParser::fatal_error] Invalid attribute type.\n";
      exit(-1);
    }
  } 

  // Check specifically the coordinate type (it cannot be char)
  if(!strcmp(cl.types_.back(), "int")) {
    types.push_back(&typeid(int));
  } else if(!strcmp(cl.types_.back(), "int64_t")) {
    types.push_back(&typeid(int64_t));
  } else if(!strcmp(cl.types_.back(), "float")) {
    types.push_back(&typeid(float));
  } else if(!strcmp(cl.types_.back(), "double")) {
    types.push_back(&typeid(double));
  } else {
    std::cerr << "[TileDB::CmdParser::fatal_error] Invalid coordinates type.\n";
    exit(-1);
  }
 
  return types;
}
