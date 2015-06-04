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
#include "csv_file.h"
#include "special_values.h"
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
  if((cl.arg_bitmap_ & CL_ARRAY_NAMES_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if((cl.arg_bitmap_ | PS_CLEAR_ARRAY_BITMAP) != PS_CLEAR_ARRAY_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant options"
              << " provided.\n";
    exit(-1);
  }

  // Check array names (no more than one)
  std::vector<std::string> array_names = check_array_names(cl);
  if(array_names.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array"
              << " names provided.\n";
    exit(-1);
  }
}

const ArraySchema* CmdParser::parse_define_array(const CommandLine& cl) const {
  // Mandatory arguments
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAMES_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ATTRIBUTE_NAMES_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Attribute names not"
              << " provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_DIM_NAMES_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Dimension names not"
              << " provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_DIM_DOMAINS_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Dimension domains not"
              << " provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_TYPES_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Attribute and dimension"
              << " types not provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if((cl.arg_bitmap_ | PS_DEFINE_ARRAY_BITMAP) != PS_DEFINE_ARRAY_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant options"
              << " provided.\n";
    exit(-1);
  }

  // Check array names (no more than one)
  std::vector<std::string> array_names = check_array_names(cl); 
  if(array_names.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array names"
              << " provided.\n";
    exit(-1);
  } 

  // Check attribute names
  std::vector<std::string> attribute_names = check_attribute_names(cl);

  // Check dimension names
  std::vector<std::string> dim_names = check_dim_names(cl, attribute_names);

  // Check dimension domains
  std::vector<std::pair<double, double> > dim_domains = 
      check_dim_domains(cl, dim_names.size());

  // Check types
  std::pair<std::vector<const std::type_info*>,
            std::vector<int> >
      types_val_num = check_types(cl, attribute_names.size());

  // Check cell and tile order
  ArraySchema::CellOrder cell_order = check_cell_order(cl);
  ArraySchema::TileOrder tile_order = check_tile_order(cl);

  // Check capacity and consolidation step
  int64_t capacity = check_capacity(cl);
  int consolidation_step = check_consolidation_step(cl);

  // Check tile extents
  std::vector<double> tile_extents = 
      check_tile_extents(cl, dim_names.size(), dim_domains); 
 
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
        dim_domains, types_val_num.first, types_val_num.second, 
        cell_order, consolidation_step, capacity);
  } else { // Regular tiles 
    return new ArraySchema(
        cl.array_names_[0], attribute_names, dim_names, 
        dim_domains, types_val_num.first, types_val_num.second,  
        tile_order, tile_extents, 
        consolidation_step, capacity, cell_order);
  }
}

void CmdParser::parse_delete_array(const CommandLine& cl) const {
  // Check if correct arguments have been ginen
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAMES_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if((cl.arg_bitmap_ | PS_DELETE_ARRAY_BITMAP) != PS_DELETE_ARRAY_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant options"
              << " provided.\n";
    exit(-1);
  }

  // Check array names (no more than one)
  std::vector<std::string> array_names = check_array_names(cl);
  if(array_names.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array"
              << " names provided.\n";
    exit(-1);
  }
}

void CmdParser::parse_export_to_csv(
    const CommandLine& cl,
    std::vector<std::string>& dim_names,
    std::vector<std::string>& attribute_names,
    bool& reverse) const {
  // Check if correct arguments have been given
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAMES_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_FILENAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] CSV file name not"
              << " provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if((cl.arg_bitmap_ | PS_EXPORT_TO_CSV_BITMAP) != PS_EXPORT_TO_CSV_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant options"
              << " provided.\n";
    exit(-1);
  }

  // Check array names (no more than one)
  std::vector<std::string> array_names = check_array_names(cl);
  if(array_names.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array"
              << " names provided.\n";
    exit(-1);
  }

  // Get dimension and attribute names
  dim_names = get_dim_names(cl);
  attribute_names = get_attribute_names(cl);

  // Check reverse mode
  reverse = check_reverse(cl);
}

void CmdParser::parse_load_csv(const CommandLine& cl) const {
  // Check if correct arguments have been ginen
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAMES_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_FILENAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] CSV file name not"
              << " provided.\n";
    exit(-1);
  }

  // Check array names (no more than one)
  std::vector<std::string> array_names = check_array_names(cl);
  if(array_names.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array"
              << " names provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if((cl.arg_bitmap_ | PS_LOAD_CSV_BITMAP) != PS_LOAD_CSV_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant options"
              << " provided.\n";
    exit(-1);
  }
}

void CmdParser::parse_load_sorted_bin(const CommandLine& cl) const {
  // Check if correct arguments have been ginen
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAMES_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_FILENAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Directory name not"
              << " provided.\n";
    exit(-1);
  }

  // Check array names (no more than one)
  std::vector<std::string> array_names = check_array_names(cl);
  if(array_names.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array"
              << " names provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if((cl.arg_bitmap_ | PS_LOAD_SORTED_BIN_BITMAP) != 
     PS_LOAD_SORTED_BIN_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant options"
              << " provided.\n";
    exit(-1);
  }

  // Check soundness of directory
  std::string dirname = absolute_path(cl.filename_);
  if(!is_dir(dirname)) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Directory does not exist.\n";
    exit(-1);
  }
}

void CmdParser::parse_show_array_schema(const CommandLine& cl) const {
  // Check if correct arguments have been ginen
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAMES_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }

  // Check array names (no more than one)
  std::vector<std::string> array_names = check_array_names(cl);
  if(array_names.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array"
              << " names provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if((cl.arg_bitmap_ | PS_SHOW_ARRAY_SCHEMA_BITMAP) != 
     PS_SHOW_ARRAY_SCHEMA_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant options"
              << " provided.\n";
    exit(-1);
  }
}

std::vector<double> CmdParser::parse_subarray(
    const CommandLine& cl,
    std::vector<std::string>& attribute_names,
    bool& reverse) const {
  // Check if correct arguments have been ginen
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAMES_BITMAP) == 0) {
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

  // Check array names (no more than one)
  std::vector<std::string> array_names = check_array_names(cl);
  if(array_names.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array"
              << " names provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if((cl.arg_bitmap_ | PS_SUBARRAY_BITMAP) != PS_SUBARRAY_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant options"
              << " provided.\n";
    exit(-1);
  }

  // Get attribute names
  attribute_names = get_attribute_names(cl);
  if(!no_duplicates(attribute_names)) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Duplicate attribute names"
              << " provided.\n";
    exit(-1);
  }

  // Check reverse mode
  reverse = check_reverse(cl);

  // Return range
  return check_range(cl); 
}

void CmdParser::parse_update_csv(const CommandLine& cl) const {
  // Check if correct arguments have been ginen
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Workspace not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAMES_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Array name not provided.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_FILENAME_BITMAP) == 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] File name not provided.\n";
    exit(-1);
  }

  // Check array names (no more than one)
  std::vector<std::string> array_names = check_array_names(cl);
  if(array_names.size() > 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] More than one array"
              << " names provided.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if((cl.arg_bitmap_ | PS_UPDATE_BITMAP) != PS_UPDATE_BITMAP) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Redundant options"
              << " provided.\n";
    exit(-1);
  }
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

std::vector<std::string> CmdParser::check_array_names(
    const CommandLine& cl) const {
  std::vector<std::string> array_names;

  // Create a CSV line with the array names
  CSVLine csv_line;
  for(int i=0; i<cl.array_names_.size(); ++i) 
    csv_line << cl.array_names_[i];

  // Check array name validity
  std::string array_name;
  while(csv_line >> array_name) {  
    if(!is_valid_name(array_name.c_str())) {
      std::cerr << "[TileDB::CmdParser::fatal_error] The array name can contain"
                << " only alphanumerics or '_'.\n";
      exit(-1);
    }
    array_names.push_back(array_name);
  }

  // Check for duplicate array names
  if(!no_duplicates(array_names)) {
    std::cerr << "[TileDB::CmdParser::fatal_error] Duplicate array names"
              << " array names provided.\n";
    exit(-1);
  }

  return array_names;
}

std::vector<std::string> CmdParser::check_attribute_names(
    const CommandLine& cl) const {
  std::vector<std::string> attribute_names;

  // Create a CSV line with the attribute names
  CSVLine csv_line;
  for(int i=0; i<cl.attribute_names_.size(); ++i) 
    csv_line << cl.attribute_names_[i];

  // Check attribute name validity
  std::string attribute_name;
  while(csv_line >> attribute_name) {
    if(!is_valid_name(attribute_name.c_str())) {
      std::cerr << "[TileDB::CmdParser::fatal_error] The attribute name can" 
                << " contain only alphanumerics or '_'.\n";
      exit(-1);
    }
    attribute_names.push_back(attribute_name);
  }

  // Check for duplicate attribute names
  if(!no_duplicates(attribute_names)) {
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

  // Consolidation step must be positive
  if(consolidation_step <= 0) {
    std::cerr << "[TileDB::CmdParser::fatal_error] The consolidation step must"
              << " be positive.\n"; 
    exit(-1);
  }

  return consolidation_step;
}

std::vector<std::pair<double, double> > CmdParser::check_dim_domains(
   const CommandLine& cl, int dim_num) const {
  std::vector<std::pair<double, double> > dim_domains;

  // Create a CSV line with all the dimension domains
  CSVLine csv_line;
  for(int i=0; i<cl.dim_domains_.size(); ++i)
    csv_line << cl.dim_domains_[i];

  // Check the number of domain bounds
  if(csv_line.values().size() != 2*dim_num) { 
    std::cerr << "[TileDB::CmdParser::fatal_error] The number of domain bounds"
              << " does not agree with the number of dimensions. There should"
              << " be a lower and an upper bound per dimension.\n";
    exit(-1);
  }

  // Check if the domain bounds are real numbers, and if the lower bound
  // is smaller than or equal to the upper.
  double lower, upper; 
  std::string str_lower, str_upper;
  for(int i=0; i<dim_num; ++i) {
    csv_line >> str_lower;
    csv_line >> str_upper;
    if(!is_real(str_lower.c_str()) || !is_real(str_upper.c_str())) {
      std::cerr << "[TileDB::CmdParser::fatal_error] The domain bounds must be" 
                << " real numbers.\n";
      exit(-1);
    } else {
      sscanf(str_lower.c_str(), "%lf", &lower);
      sscanf(str_upper.c_str(), "%lf", &upper);
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

  // Create a CSV line with all dimension names
  CSVLine csv_line;
  for(int i=0; i<cl.dim_names_.size(); ++i)     
    csv_line << cl.dim_names_[i];

  // Check dimension name validity
  std::string dim_name;
  while(csv_line >> dim_name) {
    if(!is_valid_name(dim_name.c_str())) {
      std::cerr << "[TileDB::CmdParser::fatal_error] The dimension names can" 
                << " contain only alphanumerics or '_'.\n";
      exit(-1);
    }
    dim_names.push_back(dim_name);
  }

  // Check for duplicate dimension names
  std::set<std::string> set_dim_names(dim_names.begin(), 
                                      dim_names.end());
  if(set_dim_names.size() != dim_names.size()) {
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

std::vector<double> CmdParser::check_range(const CommandLine& cl) const {
  // Create a CSV line with all the range bounds
  CSVLine csv_line;  
  for(int i=0; i<cl.range_.size(); ++i)
    csv_line << cl.range_[i];

  // Check the number of domain bounds
  if(csv_line.values().size() % 2 != 0) { 
    std::cerr << "[TileDB::CmdParser::fatal_error] The number of range bounds"
              << " must be even.\n";
    exit(-1);
  }

  // Check if the range bounds are real numbers, and if the lower is bound
  // is smaller than or equal to the upper.
  int dim_num = csv_line.values().size()/2;
  std::vector<double> range;
  range.reserve(2*dim_num);
  double lower, upper; 
  std::string str_lower, str_upper;
  for(int i=0; i<dim_num; ++i) {
    csv_line >> str_lower;
    csv_line >> str_upper;
    if(!is_real(str_lower.c_str()) || !is_real(str_upper.c_str())) {
      std::cerr << "[TileDB::CmdParser::fatal_error] The range bounds must be" 
                << " real numbers.\n";
      exit(-1);
    } else {
      sscanf(str_lower.c_str(), "%lf", &lower);
      sscanf(str_upper.c_str(), "%lf", &upper);
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

bool CmdParser::check_reverse(const CommandLine& cl) const {
  // By default, the mode is "normal"
  if(cl.mode_ == NULL || !strcmp(cl.mode_, "normal")) { 
    return false;
  } else if(!strcmp(cl.mode_, "reverse")) {
    return true;
  } else { // Error
    std::cerr << "[TileDB::CmdParser::fatal_error] Unknown mode '"
              << cl.mode_  << "'.\n";
    exit(-1);
  }
}

std::vector<double> CmdParser::check_tile_extents(
    const CommandLine& cl,
    int dim_num,
    const std::vector<std::pair<double, double> >& dim_domains) const {
  std::vector<double> tile_extents;

  // No extents = irregular tiles
  if(cl.tile_extents_.size() == 0)
    return tile_extents;

  // Create a CSV line with all the tile extents
  CSVLine csv_line; 
  for(int i=0; i<cl.tile_extents_.size(); ++i)
    csv_line << cl.tile_extents_[i];
   
  // Check if the number of tile extents is the same as the number of dimensions
  if(csv_line.values().size() != dim_num) {
    std::cerr << "[TileDB::CmdParser::fatal_error] The number of tile extents"
              << " must be the same as the number of dimensions.\n";
    exit(-1);
  }

  // Check if the tile extents are real numbers
  double tile_extent;
  std::string str_tile_extent;
  while(csv_line >> str_tile_extent) {
    if(!is_real(str_tile_extent.c_str())) {
      std::cerr << "[TileDB::CmdParser::fatal_error] The tile extents must be"
                << " real numbers.\n";
      exit(-1);
    }
    sscanf(str_tile_extent.c_str(), "%lf", &tile_extent); 
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

std::pair<std::vector<const std::type_info*>,
          std::vector<int> > 
    CmdParser::check_types(const CommandLine& cl, int attribute_num) const {
  std::vector<const std::type_info*> types;
  std::vector<int> val_num;

  // Create a CSV line with all the types
  CSVLine csv_line; 
  for(int i=0; i<cl.types_.size(); ++i)
    csv_line << cl.types_[i];

  // Check soundness of attribute types
  std::string type_val_num;  // Example format: 'int' or 'int:4'
  std::string type;
  int num;
  for(int i=0; i<csv_line.values().size()-1; ++i) {
    csv_line >> type_val_num;
    char* token = strtok(&type_val_num[0], ":"); // Type extraction
    type = token;    
    token = strtok(NULL, ":"); // Number of attribute values per cell extraction
    if(token == NULL) { // Missing number of attribute values per cell
      val_num.push_back(1);
    } else {
      // Process next token
      if(!strcmp(token, "var")) { // Variable-sized cell
        val_num.push_back(VAR_SIZE);
      } else if(!is_integer(token)) { // Invalid number of attribute values per cell
        std::cerr << "[TileDB::CmdParser::fatal_error] Invalid number of"
                     " attribute values per cell (must be integer).\n";
        exit(-1);
      } else { // Valid number of attribute values per cell
        sscanf(token, "%d", &num);
        val_num.push_back(num);
      }

      // No other token should follow
      token = strtok(NULL, ":");
      if(token != NULL) {
        std::cerr << "[TileDB::CmdParser::fatal_error] Redundant arguments in"
                     " in definition of cell type.\n";
        exit(-1);
      }
    }
    
    if(type == "char") {
      types.push_back(&typeid(char));
    } else if(type == "int") {
      types.push_back(&typeid(int));
    } else if(type == "int64_t") {
      types.push_back(&typeid(int64_t));
    } else if(type == "float") {
      types.push_back(&typeid(float));
    } else if(type == "double") {
      types.push_back(&typeid(double));
    } else {
      std::cerr << "[TileDB::CmdParser::fatal_error] Invalid attribute type.\n";
      exit(-1);
    }
  } 

  // Check specifically the coordinate type (it cannot be char)
  csv_line >> type;
  if(type == "int") {
    types.push_back(&typeid(int));
  } else if(type == "int64_t") {
    types.push_back(&typeid(int64_t));
  } else if(type == "float") {
    types.push_back(&typeid(float));
  } else if(type == "double") {
    types.push_back(&typeid(double));
  } else {
    std::cerr << "[TileDB::CmdParser::fatal_error] Invalid coordinates type.\n";
    exit(-1);
  }

  // Check number of types
  if(types.size() != attribute_num + 1) {
    std::cerr << "[TileDB::CmdParser::fatal_error] The number of types should"
              << " be equal to the number of attributes plus 1 (the last"  
              << " corresponds to the coordinates).\n";
    exit(-1);
  }

  return std::pair<std::vector<const std::type_info*>,
                    std::vector<int> >(types, val_num);
}

std::vector<std::string> CmdParser::get_attribute_names(
    const CommandLine& cl) const {
  std::vector<std::string> attribute_names;

  // Create a CSV line with the attribute names
  CSVLine csv_line;
  for(int i=0; i<cl.attribute_names_.size(); ++i) 
    csv_line << cl.attribute_names_[i];

  // Get the attribute names from the CSV line
  std::string attribute_name;
  while(csv_line >> attribute_name) 
    attribute_names.push_back(attribute_name);

  return attribute_names;
}

std::vector<std::string> CmdParser::get_dim_names(
    const CommandLine& cl) const {
  std::vector<std::string> dim_names;

  // Create a CSV line with the dimension names
  CSVLine csv_line;
  for(int i=0; i<cl.dim_names_.size(); ++i) 
    csv_line << cl.dim_names_[i];

  // Get the dimension names from the CSV line
  std::string dim_name;
  while(csv_line >> dim_name) 
    dim_names.push_back(dim_name);

  return dim_names;
}
