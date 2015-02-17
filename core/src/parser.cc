/**
 * @file   parser.cc
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
 * This file implements the Parser class.
 */

#include "parser.h"
#include <ctype.h>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <algorithm>

/******************************************************
******************* PARSING METHODS *******************
******************************************************/

ArraySchema Parser::parse_define_array(const CommandLine& cl) const {
  // Check if correct arguments have been ginen
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::fatal_error] Workspace not provided."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAME_BITMAP) == 0) {
    std::cerr << "[TileDB::fatal_error] Array name not provided."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ORDER_BITMAP) == 0) {
    std::cerr << "[TileDB::fatal_error] Order not provided."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ATTRIBUTE_NAME_BITMAP) == 0) {
    std::cerr << "[TileDB::fatal_error] Attribute names not provided."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_DIM_NAME_BITMAP) == 0) {
    std::cerr << "[TileDB::fatal_error] Dimension names not provided."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_DIM_DOMAIN_BITMAP) == 0) {
    std::cerr << "[TileDB::fatal_error] Dimension domains not provided."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_TYPE_BITMAP) == 0) {
    std::cerr << "[TileDB::fatal_error] Types not provided."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if((cl.arg_bitmap_ | PS_DEFINE_ARRAY_BITMAP) != PS_DEFINE_ARRAY_BITMAP) {
    std::cerr << "[TileDB::fatal_error] Redundant arguments provided."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }

  // Check if capacity was given for the case of regular tiles
  if(cl.capacity_ != NULL && cl.tile_extents_.size() != 0) {
    std::cerr << "[TileDB::fatal_error] Capacity applies only to irregular"
              << " tiles (tile extents should not be defined)."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }

  // Prepare arguments for ArraySchema object creation and check soundness
  std::string array_name = check_array_name(cl); 
  std::vector<std::string> attribute_names = check_attribute_names(cl);
  std::vector<std::string> dim_names = check_dim_names(cl, attribute_names);
  std::vector<std::pair<double, double> > dim_domains = 
      check_dim_domains(cl, dim_names);
  std::vector<const std::type_info*> types = check_types(cl, attribute_names);
  ArraySchema::Order order = check_order(cl);
  uint64_t capacity = check_capacity(cl);
  std::vector<double> tile_extents = 
      check_tile_extents(cl, dim_names, dim_domains); 
  
  // Return array schema
  if(capacity != 0) {
    if(tile_extents.size() == 0)
      return ArraySchema(array_name, attribute_names, dim_names, dim_domains,
                         types, order, capacity);
    else
      return ArraySchema(array_name, attribute_names, dim_names, dim_domains,
                         types, order, tile_extents, capacity);
  } else {
    if(tile_extents.size() == 0)
      return ArraySchema(array_name, attribute_names, dim_names, dim_domains,
                         types, order);
    else
      return ArraySchema(array_name, attribute_names, dim_names, dim_domains,
                         types, order, tile_extents);
  }
}

void Parser::parse_load(const CommandLine& cl) const {
  // Check if correct arguments have been ginen
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::fatal_error] Workspace not provided."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAME_BITMAP) == 0) {
    std::cerr << "[TileDB::fatal_error] Array name not provided."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_FILENAME_BITMAP) == 0) {
    std::cerr << "[TileDB::fatal_error] File name not provided."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if((cl.arg_bitmap_ | PS_LOAD_BITMAP) != PS_LOAD_BITMAP) {
    std::cerr << "[TileDB::fatal_error] Redundant arguments provided."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }
}

void Parser::parse_update(const CommandLine& cl) const {
  // Check if correct arguments have been ginen
  if((cl.arg_bitmap_ & CL_WORKSPACE_BITMAP) == 0) {
    std::cerr << "[TileDB::fatal_error] Workspace not provided."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_ARRAY_NAME_BITMAP) == 0) {
    std::cerr << "[TileDB::fatal_error] Array name not provided."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }
  if((cl.arg_bitmap_ & CL_FILENAME_BITMAP) == 0) {
    std::cerr << "[TileDB::fatal_error] File name not provided."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }

  // Check for redundant arguments
  if((cl.arg_bitmap_ | PS_UPDATE_BITMAP) != PS_UPDATE_BITMAP) {
    std::cerr << "[TileDB::fatal_error] Redundant arguments provided."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

std::string Parser::check_array_name(const CommandLine& cl) const {
  if(!is_valid_name(cl.array_name_)) {
    std::cerr << "[TileDB::fatal_error] The array name can contain only"
              << " alphanumerics or '_'."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }

  return std::string(cl.array_name_);
}

std::vector<std::string> Parser::check_attribute_names(
    const CommandLine& cl) const {
  std::vector<std::string> attribute_names;
  attribute_names.reserve(cl.attribute_names_.size());

  for(int i=0; i<cl.attribute_names_.size(); ++i) {
    if(!is_valid_name(cl.attribute_names_[i])) {
      std::cerr << "[TileDB::fatal_error] The attribute names can contain only" 
                << " alphanumerics or '_'."
                << " Type 'tiledb help' to see the TileDB User Manual.\n";
      exit(-1);
    }
    attribute_names.push_back(std::string(cl.attribute_names_[i]));
  }

  // Check for duplicate names
  std::set<std::string> set_attribute_names(attribute_names.begin(), 
                                            attribute_names.end());
  if(set_attribute_names.size() < attribute_names.size()) {
    std::cerr << "[TileDB::fatal_error] Duplicate attribute names are not"
              << " allowed."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }

  return attribute_names;
}

uint64_t Parser::check_capacity(const CommandLine& cl) const {
  if(cl.capacity_ == NULL)
    return 0;

  if(!is_positive_integer(cl.capacity_)) {
    std::cerr << "[TileDB::fatal_error] The capacity provided is not an"
              << " integer."  
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }

  uint64_t capacity;
  sscanf(cl.capacity_, "%llu", &capacity); 

  return capacity;
}

std::vector<std::pair<double, double> > Parser::check_dim_domains(
   const CommandLine& cl,
   const std::vector<std::string>& dim_names) const {
  std::vector<std::pair<double, double> > dim_domains;

  // Check the number of domain bounds
  if(cl.dim_domains_.size() != 2*cl.dim_names_.size()) { 
    std::cerr << "[TileDB::fatal_error] The number of domain bounds does not"
              << " agree with the number of dimensions. There should be a lower"
              << " and an upper bound per dimension."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }

  // Check if the domain bounds are real numbers, and if the lower is bound
  // is smaller than or equal to the upper.
  for(int i=0; i<cl.dim_names_.size(); ++i) {
    if(!is_positive_real(cl.dim_domains_[2*i]) || 
       !is_positive_real(cl.dim_domains_[2*i+1])) {
      std::cerr << "[TileDB::fatal_error] The domain bounds must be real" 
                << " numbers."
                << " Type 'tiledb help' to see the TileDB User Manual.\n";
      exit(-1);
    } else {
      double lower, upper; 
      sscanf(cl.dim_domains_[2*i], "%lf", &lower);
      sscanf(cl.dim_domains_[2*i+1], "%lf", &upper);
      if(lower > upper) {
        std::cerr << "[TileDB::fatal_error] A lower domain bound cannot be"
                  << " larger than its corresponding upper."
                  << " Type 'tiledb help' to see the TileDB User Manual.\n";
        exit(-1);
      }
      dim_domains.push_back(std::pair<double,double>(lower,upper));
    }
  }

  return dim_domains;
}

std::vector<std::string> Parser::check_dim_names(
    const CommandLine& cl, 
    const std::vector<std::string>& attribute_names) const {
  std::vector<std::string> dim_names;
  dim_names.reserve(cl.dim_names_.size());

  for(int i=0; i<cl.dim_names_.size(); ++i) {
    if(!is_valid_name(cl.dim_names_[i])) {
      std::cerr << "[TileDB::fatal_error] The dimension names can contain only" 
                << " alphanumerics or '_'."
                << " Type 'tiledb help' to see the TileDB User Manual.\n";
      exit(-1);
    }
    dim_names.push_back(std::string(cl.dim_names_[i]));
  }

  // Check for duplicate names
  std::set<std::string> set_dim_names(dim_names.begin(), 
                                      dim_names.end());
  if(set_dim_names.size() < dim_names.size()) {
    std::cerr << "[TileDB::fatal_error] Duplicate dimension names are not"
              << " allowed."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
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
    std::cerr << "[TileDB::fatal_error] A dimension cannot have the same name"
              << " as an attribute."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }

  return dim_names;
}

ArraySchema::Order Parser::check_order(const CommandLine& cl) const {
  if(!strcmp(cl.order_, "row-major")) {
    return ArraySchema::ROW_MAJOR;
  } else if(!strcmp(cl.order_, "column-major")) {
    return ArraySchema::COLUMN_MAJOR;
  } else if(!strcmp(cl.order_, "hilbert")) {
    return ArraySchema::HILBERT;
  } else {
    std::cerr << "[TileDB::fatal_error] Unknown order."  
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }
}

std::vector<double> Parser::check_tile_extents(
    const CommandLine& cl,
    const std::vector<std::string>& dim_names,
    const std::vector<std::pair<double, double> >& dim_domains) const {

  std::vector<double> tile_extents;
 
  if(cl.tile_extents_.size() == 0)
    return tile_extents;
   
  // Check if the number of tile extents is the same as the number of dimensions
  if(cl.tile_extents_.size() != dim_names.size()) {
    std::cerr << "[TileDB::fatal_error] The number of tile extents must be the"
              << " same as the number of dimensions."  
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }

  // Check if the tile extents are real numbers
  for(int i=0; i<cl.tile_extents_.size(); ++i) {
    if(!is_positive_real(cl.tile_extents_[i])) {
      std::cerr << "[TileDB::fatal_error] The tile extents must be real"
                << " numbers."
                << " Type 'tiledb help' to see the TileDB User Manual.\n";
      exit(-1);
    }
    double tile_extent;
    sscanf(cl.tile_extents_[i], "%lf", &tile_extent); 
    tile_extents.push_back(tile_extent);
  }

  // Check if each tile extent is smaller than the corresponding domain range
  for(int i=0; i<tile_extents.size(); ++i) {
    if(tile_extents[i] > dim_domains[i].second - dim_domains[i].first + 1) {
      std::cerr << "[TileDB::fatal_error] The tile extents must not exceed"
                << " their corresponding domain ranges."
                << " Type 'tiledb help' to see the TileDB User Manual.\n";
      exit(-1);
    }
  }

  return tile_extents;
}

std::vector<const std::type_info*> Parser::check_types(
    const CommandLine& cl,
    const std::vector<std::string>& attribute_names) const {
  std::vector<const std::type_info*> types;

  // Check number of types
  if(cl.types_.size() != attribute_names.size() + 1) {
    std::cerr << "[TileDB::fatal_error] The number of types should be equal to"
              << " the number of attributes plus 1 (for the coordinates)."  
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
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
      std::cerr << "[TileDB::fatal_error] Invalid attribute type."
                << " Type 'tiledb help' to see the TileDB User Manual.\n";
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
    std::cerr << "[TileDB::fatal_error] Invalid coordinates type."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }
 
  return types;
}

bool Parser::is_positive_integer(char* s) const {
  for(int i=0; s[i] != '\0'; ++i) {
    if(!isdigit(s[i]))
      return false;
  }

  return true;
}

bool Parser::is_positive_real(char* s) const {
  bool decimal_point_seen = false;

  for(int i=0; s[i] != '\0'; ++i) {
    if(s[i] == '.') {
      if(!decimal_point_seen) 
        decimal_point_seen = true;
      else
        return false;
    } else if(!isdigit(s[i]))
      return false;
  }

  return true;
}

bool Parser::is_valid_name(char* s) const {
  for(int i=0; s[i] != '\0'; ++i) {
    if(!isalnum(s[i]) && s[i] != '_')
      return false;
  }

  return true;
}
