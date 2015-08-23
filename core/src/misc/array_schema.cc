/**
 * @file   array_schema.cc
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
 * This file implements the ArraySchema class.
 */

#include "array_schema.h"
#include "csv_line.h"
#include "hilbert_curve.h"
#include "utils.h"
#include <assert.h>
#include <string.h>
#include <math.h>
#include <algorithm>
#include <iostream>
#include <set>
#include <stdio.h>
#include <time.h>

/******************************************************
************ CONSTRUCTORS & DESTRUCTORS ***************
******************************************************/

ArraySchema::ArraySchema() {
  coords_ = NULL;
}

ArraySchema::ArraySchema(
    const std::string& array_name,
    const std::vector<std::string>& attribute_names,
    const std::vector<std::string>& dim_names,
    const std::vector<std::pair<double, double> >& dim_domains,
    const std::vector<const std::type_info*>& types,
    const std::vector<int>& val_num,
    CellOrder cell_order,
    int64_t capacity,
    int consolidation_step) {
  assert(attribute_names.size() > 0);
  assert(dim_names.size() > 0);
  assert(attribute_names.size()+1 == types.size());
  assert(dim_names.size() == dim_domains.size());
  assert(capacity > 0);
  assert(consolidation_step > 0);
  assert(val_num.size() == attribute_names.size());
#ifndef NDEBUG
  for(int i=0; i<dim_domains.size(); ++i) 
    assert(dim_domains[i].first <= dim_domains[i].second);
#endif
 
  array_name_ = array_name;
  attribute_names_ = attribute_names;
  dim_names_ = dim_names;
  dim_domains_ = dim_domains;
  types_ = types;
  val_num_ = val_num;
  cell_order_ = cell_order;
  tile_order_ = TO_NONE;
  consolidation_step_ = consolidation_step;
  capacity_ = capacity;
  dim_num_ = dim_names_.size();
  attribute_num_ = attribute_names_.size();
  // Name for the extra coordinate attribute
  attribute_names_.push_back(AS_COORDINATES_NAME);

  // Set cell sizes
  cell_size_ = 0;
  cell_sizes_.resize(attribute_num_+1);
  for(int i=0; i<= attribute_num_; ++i) {
    cell_sizes_[i] = compute_cell_size(i);
    if(cell_sizes_[i] == VAR_SIZE)
      cell_size_ = VAR_SIZE;
    if(cell_size_ != VAR_SIZE)
      cell_size_ += cell_sizes_[i]; 
  }

  coords_ = malloc(cell_sizes_[attribute_num_]);

  // Set type sizes
  type_sizes_.resize(attribute_num_ + 1);
  for(int i=0; i<=attribute_num_; ++i)
    type_sizes_[i] = compute_type_size(i);

  // Set compression
  for(int i=0; i<= attribute_num_; ++i)
    compression_.push_back(NONE);

  compute_hilbert_cell_bits();
}

ArraySchema::ArraySchema(
    const std::string& array_name,
    const std::vector<std::string>& attribute_names,
    const std::vector<std::string>& dim_names,
    const std::vector<std::pair<double, double> >& dim_domains,
    const std::vector<const std::type_info*>& types,
    const std::vector<int>& val_num,
    const std::vector<double>& tile_extents,
    CellOrder cell_order,
    TileOrder tile_order,
    int consolidation_step) {
  assert(attribute_names.size() > 0);
  assert(dim_names.size() > 0);
  assert(tile_extents.size() > 0);
  assert(attribute_names.size()+1 == types.size());
  assert(dim_names.size() == dim_domains.size());
  assert(dim_names.size() == tile_extents.size());
  assert(consolidation_step > 0);
  assert(val_num.size() == attribute_names.size());
#ifndef NDEBUG
  for(int i=0; i<dim_domains.size(); ++i) 
    assert(dim_domains[i].first <= dim_domains[i].second);
  for(int i=0; i<tile_extents.size(); ++i) {
    assert(tile_extents[i] != 0);
    assert(tile_extents[i] <= (dim_domains[i].second - 
                               dim_domains[i].first + 1));
  }
#endif

  array_name_ = array_name;
  attribute_names_ = attribute_names;
  dim_names_ = dim_names;
  dim_domains_ = dim_domains;
  types_ = types;
  val_num_ = val_num;
  tile_order_ = tile_order;
  cell_order_ = cell_order;
  consolidation_step_ = consolidation_step;
  tile_extents_ = tile_extents; 
  dim_num_ = dim_names_.size();
  attribute_num_ = attribute_names_.size();
  // Name for the extra coordinate attribute
  attribute_names_.push_back(AS_COORDINATES_NAME); 

  // Set cell sizes
  cell_size_ = 0;
  cell_sizes_.resize(attribute_num_+1);
  for(int i=0; i<= attribute_num_; ++i) {
    cell_sizes_[i] = compute_cell_size(i);
    if(cell_sizes_[i] == VAR_SIZE)
      cell_size_ = VAR_SIZE;
    if(cell_size_ != VAR_SIZE)
      cell_size_ += cell_sizes_[i]; 
  }

  coords_ = malloc(cell_sizes_[attribute_num_]);

  // Set type sizes
  type_sizes_.resize(attribute_num_ + 1);
  for(int i=0; i<=attribute_num_; ++i)
    type_sizes_[i] = compute_type_size(i);

  // Set compression
  for(int i=0; i<= attribute_num_; ++i)
    compression_.push_back(NONE);

  compute_hilbert_cell_bits();
  compute_hilbert_tile_bits();
  compute_tile_id_offsets();
}

ArraySchema::~ArraySchema() {
  if(coords_ != NULL)
    free(coords_);
}

/******************************************************
********************** ACCESSORS **********************
******************************************************/

const std::string& ArraySchema::array_name() const {
  return array_name_;
}

int ArraySchema::attribute_id(const std::string& attribute_name) const {
  for(int i=0; i<attribute_num_; ++i) {
    if(attribute_names_[i] == attribute_name)
      return i;
  }

  // Attribute not found
  return -1;
}

std::vector<int> ArraySchema::attribute_ids() const {
  std::vector<int> attribute_ids;

  for(int i=0; i<=attribute_num_; ++i)
    attribute_ids.push_back(i);

  return attribute_ids;
}

const std::string& ArraySchema::attribute_name(int i) const {
  assert(i>= 0 && i <= attribute_num_);

  return attribute_names_[i];
}

int ArraySchema::attribute_num() const {
  return attribute_num_;
}

int64_t ArraySchema::capacity() const {
  assert(tile_extents_.size() == 0);

  return capacity_;
}

ArraySchema::CellOrder ArraySchema::cell_order() const {
  return cell_order_;
}

size_t ArraySchema::cell_size() const {
  return cell_size_;
}

size_t ArraySchema::cell_size(int i) const {
  return cell_sizes_[i];
}

size_t ArraySchema::cell_size(const std::vector<int>& attribute_ids) const {
  assert(valid_attribute_ids(attribute_ids));
  assert(no_duplicates(attribute_ids));

  if(attribute_ids.size() == attribute_num_ + 1)
    return cell_size_;

  size_t cell_size = 0;
  for(int i=0; i<attribute_ids.size(); ++i) {
    if(cell_sizes_[attribute_ids[i]] == VAR_SIZE)
      return VAR_SIZE;
    cell_size += cell_sizes_[attribute_ids[i]];
  }

  return cell_size;
}

size_t ArraySchema::coords_size() const {
  return cell_sizes_[attribute_num_];
}

const std::type_info* ArraySchema::coords_type() const {
  return type(attribute_num_);
}

int ArraySchema::consolidation_step() const {
  return consolidation_step_;
}

const ArraySchema::DimDomains& ArraySchema::dim_domains() const {
  return dim_domains_;
}

int ArraySchema::dim_id(const std::string& dim_name) const {
  for(int i=0; i<dim_num_; ++i) {
    if(dim_names_[i] == dim_name)
      return i;
  }

  // Dimension not found
  return -1;
}

int ArraySchema::dim_num() const {
  return dim_num_;
}

// FORMAT:
// array_name_size(int) array_name(string)
// tile_order(char)
// cell_order(char)
// capacity(int64_t)
// consolidation_step(int)
// attribute_num(int) 
//     attribute_name_size#1(int) attribute_name#1(string)
//     attribute_name_size#2(int) attribute_name#2(string) 
//     ...
// dim_num(int) 
//    dim_name_size#1(int) dim_name#1(string)
//    dim_name_size#2(int) dim_name#2(string)
//    ...
// dim_domain#1_low(double) dim_domain#1_high(double)
// dim_domain#2_low(double) dim_domain#2_high(double)
//  ...
// tile_extents_num(int) 
//     tile_extent#1(double) tile_extent#2(double) ... 
// type#1(char) type#2(char) ... 
// val_num#1(int) val_num#2(int) ... 
// compression#1(char) compression#2(char) ...
std::pair<const char*, size_t> ArraySchema::serialize() const {
  size_t buffer_size = 0;

  // ====== Calculation of buffer_size ======
  // Size for array_name_ 
  buffer_size += sizeof(int) + array_name_.size();
  // Size for tile_order_ and cell_order_
  buffer_size += 2 * sizeof(char);
  // Size for capacity_ 
  buffer_size += sizeof(int64_t);
  // Size for consolidation_step__ 
  buffer_size += sizeof(int);
  // Size for attribute_names_ 
  buffer_size += sizeof(int);
  for(int i=0; i<attribute_num_; ++i)
    buffer_size += sizeof(int) + attribute_names_[i].size();
  // Size for dim_names_
  buffer_size += sizeof(int);
  for(int i=0; i<dim_num_; ++i)
    buffer_size += sizeof(int) + dim_names_[i].size();
  // Size for dim_domains_
  buffer_size += 2 * dim_num_ * sizeof(double);
  // Size for tile_extents_ 
  // (recall that an array with irregular tiles does not have tile extents)
  buffer_size += sizeof(int) + tile_extents_.size() * sizeof(double);
  // Size for types_
  buffer_size += (attribute_num_+1) * sizeof(char);
  // Size for val_num_
  buffer_size += attribute_num_ * sizeof(int);
  // Size for compression_
  buffer_size += (attribute_num_+1) * sizeof(char);

  char* buffer = new char[buffer_size];

  // ====== Populating the buffer ======
  size_t offset = 0;
  // Copy array_name_
  int array_name_size = array_name_.size();
  assert(offset < buffer_size);
  memcpy(buffer + offset, &array_name_size, sizeof(int));
  offset += sizeof(int);
  assert(offset < buffer_size);
  memcpy(buffer + offset, array_name_.c_str(), array_name_size);
  offset += array_name_size;
  // Copy tile_order_ and cell_order_
  char tile_order = tile_order_;
  assert(offset < buffer_size);
  memcpy(buffer + offset, &tile_order, sizeof(char));
  offset += sizeof(char);
  char cell_order = cell_order_;
  assert(offset < buffer_size);
  memcpy(buffer + offset, &cell_order, sizeof(char));
  offset += sizeof(char);
  // Copy capacity_
  assert(offset < buffer_size);
  memcpy(buffer + offset, &capacity_, sizeof(int64_t));
  offset += sizeof(int64_t);
  // Copy consolidation_step_
  assert(offset < buffer_size);
  memcpy(buffer + offset, &consolidation_step_, sizeof(int));
  offset += sizeof(int);
  // Copy attribute_names_
  assert(offset < buffer_size);
  memcpy(buffer + offset, &attribute_num_, sizeof(int));
  offset += sizeof(unsigned int);
  int attribute_name_size;
  for(int i=0; i<attribute_num_; i++) {
    attribute_name_size = attribute_names_[i].size();
    assert(offset < buffer_size);
    memcpy(buffer + offset, &attribute_name_size, sizeof(int)); 
    offset += sizeof(int);
    assert(offset < buffer_size);
    memcpy(buffer + offset, attribute_names_[i].c_str(), attribute_name_size); 
    offset += attribute_name_size;
  }
  // Copy dim_names_
  assert(offset < buffer_size);
  memcpy(buffer + offset, &dim_num_, sizeof(int));
  offset += sizeof(int);
  int dim_name_size;
  for(int i=0; i<dim_num_; i++) {
    dim_name_size = dim_names_[i].size();
    assert(offset < buffer_size);
    memcpy(buffer + offset, &dim_name_size, sizeof(int)); 
    offset += sizeof(int);
    assert(offset < buffer_size);
    memcpy(buffer + offset, dim_names_[i].c_str(), dim_name_size); 
    offset += dim_name_size;
  }
  // Copy dim_domains_
  for(int i=0; i<dim_num_; i++) {
    assert(offset < buffer_size);
    memcpy(buffer + offset, &dim_domains_[i].first, sizeof(double));
    offset += sizeof(double);
    assert(offset < buffer_size);
    memcpy(buffer + offset, &dim_domains_[i].second, sizeof(double));
    offset += sizeof(double);
  } 
  // Copy tile_extents_
  int tile_extents_num = tile_extents_.size();
  assert(offset < buffer_size);
  memcpy(buffer + offset, &tile_extents_num, sizeof(int));
  offset += sizeof(int);
  for(int i=0; i<tile_extents_num; i++) {
    assert(offset < buffer_size);
    memcpy(buffer + offset, &tile_extents_[i], sizeof(double));
    offset += sizeof(double);
  }
  // Copy types_
  char type; 
  for(int i=0; i<=attribute_num_; i++) {
    if(*types_[i] == typeid(char))
      type = CHAR;
    else if(*types_[i] == typeid(int))
      type = INT;
    else if(*types_[i] == typeid(int64_t))
      type = INT64_T;
    else if(*types_[i] == typeid(float))
      type = FLOAT;
    else if(*types_[i] == typeid(double))
      type = DOUBLE;
    assert(offset < buffer_size);
    memcpy(buffer + offset, &type, sizeof(char));
    offset += sizeof(char);
  }
  // Copy val_num_
  for(int i=0; i<attribute_num_; i++) {
    memcpy(buffer + offset, &val_num_[i], sizeof(int));
    offset += sizeof(int);
  }
  // Copy compression_
  char compression; 
  for(int i=0; i<=attribute_num_; ++i) {
    compression = compression_[i];
    assert(offset < buffer_size);
    memcpy(buffer + offset, &compression, sizeof(char));
    offset += sizeof(char);
  }
  assert(offset == buffer_size);

  return std::pair<char*, size_t>(buffer, buffer_size);
}

std::string ArraySchema::serialize_csv() const {
  CSVLine schema;
  // Copy array name
  schema << array_name_;
  // Copy number of attributes / attribute names
  schema << attribute_num_;
  for(int i=0; i < attribute_num_; ++i) {
    schema << attribute_names_[i];
  }
  // Copy number of dimensions / dimension names
  schema << dim_num_;
  for(int i=0; i < dim_num_; ++i) {
    schema << dim_names_[i];
  }
  // Copy dimension domains
  for(int i=0; i < dim_num_; ++i) {
    schema << dim_domains_[i].first;
    schema << dim_domains_[i].second;
  }
  // Copy attribute types
  std::string typestr;
  for(int i=0; i < attribute_num_; ++i) {
    if(*types_[i] == typeid(char))
      typestr = std::string("char:");
    else if(*types_[i] == typeid(int))
      typestr = std::string("int:");
    else if(*types_[i] == typeid(int64_t))
      typestr = std::string("int64:");
    else if(*types_[i] == typeid(float))
      typestr = std::string("float:");
    else if(*types_[i] == typeid(double))
      typestr = std::string("double:");
    
    if(val_num_[i] == VAR_SIZE)
      typestr += "var";
    else
      typestr += std::to_string(val_num_[i]); 

    schema << typestr;
  }
  // Copy coordinate type
  if(*types_[attribute_num_] == typeid(char))
    schema << "char"; 
  else if(*types_[attribute_num_] == typeid(int))
    schema << "int";
  else if(*types_[attribute_num_] == typeid(int64_t))
    schema << "int64";
  else if(*types_[attribute_num_] == typeid(float))
    schema << "float";
  else if(*types_[attribute_num_] == typeid(double))
    schema << "double";
  
  // Copy extent for regular Tiles 
  if(has_irregular_tiles()) {
    schema << "*";
  } else {
    for(int i=0; i < dim_num_; ++i) {
      schema << tile_extents_[i];
    }
  }
  
  // Copy cell order
  if(cell_order_ == CO_COLUMN_MAJOR)
    schema << "column-major";
  else if(cell_order_ == CO_ROW_MAJOR)
    schema << "row-major";
  else if(cell_order_ == CO_HILBERT)
    schema << "hilbert";
  
  // Copy tile order (only for regular Tiles 
  if(has_irregular_tiles())
    schema << "*";
  else if(tile_order_ == TO_COLUMN_MAJOR)
    schema << "column-major";
  else if(tile_order_ == TO_ROW_MAJOR)
    schema << "row-major";
  else if(tile_order_ == TO_HILBERT)
    schema << "hilbert";

  // Copy capacity for irregular Tiles
  if(has_irregular_tiles())
    schema << capacity_;
  else
    schema << "*";

  // Copy consolidation step (default 1)  
  schema << consolidation_step_; 

  return schema.c_str();
}

int ArraySchema::smallest_attribute() const {
  int smallest_attribute = 0;
  size_t smallest_cell_size = this->cell_size(0);
  size_t smallest_type_size = this->type_size(0);
  size_t cell_size, type_size;

  // Check for smallest cell size
  for(int i=1; i<attribute_num_; ++i) {
    cell_size = this->cell_size(i);
    if(cell_size != VAR_SIZE && 
       (smallest_cell_size == VAR_SIZE || cell_size < smallest_cell_size)) {
      smallest_cell_size = cell_size;
      smallest_attribute = i; 
    }
  }

  // If all cell sizes are varible, choose the smallest type
  if(smallest_cell_size == VAR_SIZE) {  
    assert(smallest_attribute == 0);
    for(int i=1; i<attribute_num_; ++i) {
      type_size = this->type_size(i);
      if(type_size < smallest_type_size) {
        smallest_type_size = type_size;
        smallest_attribute = i; 
      }
    }
  }

  return smallest_attribute;
}

const std::vector<double>& ArraySchema::tile_extents() const {
  return tile_extents_;
}

ArraySchema::TileOrder ArraySchema::tile_order() const {
  return tile_order_;
}

const std::type_info* ArraySchema::type(int i) const {
  assert(i>=0 && i<=attribute_num_);

  return types_[i];
}

size_t ArraySchema::type_size(int i) const {
  assert(i>=0 && i<=attribute_num_);

  return type_sizes_[i];
}

int ArraySchema::val_num(int attribute_id) const {
  assert(attribute_id >=0 && attribute_id < attribute_num_);

  return val_num_[attribute_id];
}

bool ArraySchema::var_size() const {
  return cell_size_ == VAR_SIZE;
}

/******************************************************
*********************** MUTATORS **********************
******************************************************/

// FORMAT:
// array_name_size(int) array_name(string)
// tile_order(char)
// cell_order(char)
// capacity(int64_t)
// consolidation_step(int)
// attribute_num(int) 
//     attribute_name_size#1(int) attribute_name#1(string)
//     attribute_name_size#2(int) attribute_name#2(string) 
//     ...
// dim_num(int) 
//    dim_name_size#1(int) dim_name#1(string)
//    dim_name_size#2(int) dim_name#2(string)
//    ...
// dim_domain#1_low(double) dim_domain#1_high(double)
// dim_domain#2_low(double) dim_domain#2_high(double)
//  ...
// tile_extents_num(int) 
//     tile_extent#1(double) tile_extent#2(double) ... 
// type#1(char) type#2(char) ... 
// val_num#1(int) val_num#2(int) ... 
// compression#1(char) compression#2(char) ...
void ArraySchema::deserialize(const char* buffer, size_t buffer_size) {
  size_t offset = 0;

  // Load array_name_ 
  int array_name_size;
  assert(offset < buffer_size);
  memcpy(&array_name_size, buffer + offset, sizeof(int));
  offset += sizeof(int);
  array_name_.resize(array_name_size);
  assert(offset < buffer_size);
  memcpy(&array_name_[0], buffer + offset, array_name_size);
  offset += array_name_size;
  // Load tile_order_ and cell_order_
  char tile_order;
  assert(offset < buffer_size);
  memcpy(&tile_order, buffer + offset, sizeof(char));
  tile_order_ = static_cast<TileOrder>(tile_order);  
  offset += sizeof(char);
  char cell_order;
  assert(offset < buffer_size);
  memcpy(&cell_order, buffer + offset, sizeof(char));
  cell_order_ = static_cast<CellOrder>(cell_order);  
  offset += sizeof(char);
  // Load capacity_
  assert(offset < buffer_size);
  memcpy(&capacity_, buffer + offset, sizeof(int64_t));
  offset += sizeof(int64_t);
  // Load consolidation_step_
  assert(offset < buffer_size);
  memcpy(&consolidation_step_, buffer + offset, sizeof(int));
  offset += sizeof(int);
  // Load attribute_names_
  assert(offset < buffer_size);
  memcpy(&attribute_num_, buffer + offset, sizeof(int));
  offset += sizeof(int);
  attribute_names_.resize(attribute_num_);
  int attribute_name_size;
  for(int i=0; i<attribute_num_; ++i) {
    assert(offset < buffer_size);
    memcpy(&attribute_name_size, buffer+offset, sizeof(int)); 
    offset += sizeof(int);
    attribute_names_[i].resize(attribute_name_size);
    assert(offset < buffer_size);
    memcpy(&attribute_names_[i][0], 
           buffer + offset, attribute_name_size);
    offset += attribute_name_size;
  }
  // Load dim_names_
  assert(offset < buffer_size);
  memcpy(&dim_num_, buffer + offset, sizeof(int));
  offset += sizeof(int);
  dim_names_.resize(dim_num_);
  int dim_name_size;
  for(int i=0; i<dim_num_; ++i) {
    assert(offset < buffer_size);
    memcpy(&dim_name_size, buffer + offset, sizeof(int)); 
    offset += sizeof(int);
    dim_names_[i].resize(dim_name_size);
    assert(offset < buffer_size);
    memcpy(&dim_names_[i][0], buffer + offset, dim_name_size); 
    offset += dim_name_size;
  }
  // Load dim_domains
  dim_domains_.resize(dim_num_);
  for(int i=0; i<dim_num_; ++i) {
    assert(offset < buffer_size);
    memcpy(&dim_domains_[i].first, buffer + offset, sizeof(double));
    offset += sizeof(double);
    assert(offset < buffer_size);
    memcpy(&dim_domains_[i].second, buffer + offset, sizeof(double));
    offset += sizeof(double);
  } 
  // Load tile_extents_
  int tile_extents_num;
  assert(offset < buffer_size);
  memcpy(&tile_extents_num, buffer + offset, sizeof(int));
  offset += sizeof(int);
  tile_extents_.resize(tile_extents_num);
  for(int i=0; i<tile_extents_num; ++i) {
    assert(offset < buffer_size);
    memcpy(&tile_extents_[i], buffer + offset, sizeof(double));
    offset += sizeof(double);
  }
  // Load types_
  char type;
  types_.resize(attribute_num_+1); 
  type_sizes_.resize(attribute_num_+1);
  for(int i=0; i<=attribute_num_; ++i) {
    assert(offset < buffer_size);
    memcpy(&type, buffer + offset, sizeof(char));
    offset += sizeof(char);
    if(type == CHAR) {
      types_[i] = &typeid(char);
      type_sizes_[i] = sizeof(char);
    } else if(type == INT) {
      types_[i] = &typeid(int);
      type_sizes_[i] = sizeof(int);
    } else if(type == INT64_T) {
      types_[i] = &typeid(int64_t);
      type_sizes_[i] = sizeof(int64_t);
    } else if(type == FLOAT) {
      types_[i] = &typeid(float);
      type_sizes_[i] = sizeof(float);
    } else if(type == DOUBLE) {
      types_[i] = &typeid(double);
      type_sizes_[i] = sizeof(double);
    }
  }
  // Load val_num_
  val_num_.resize(attribute_num_); 
  for(int i=0; i<attribute_num_; ++i) {
    assert(offset < buffer_size);
    memcpy(&val_num_[i], buffer + offset, sizeof(int));
    offset += sizeof(int);
  }
  // Load compression_
  char compression;
  for(int i=0; i<=attribute_num_; ++i) {
    assert(offset < buffer_size);
    memcpy(&compression, buffer + offset, sizeof(char));
    offset += sizeof(char);
    compression_.push_back(static_cast<CompressionType>(compression));
  }
  assert(offset == buffer_size);
 
  // Extra coordinate attribute
  attribute_names_.push_back(AS_COORDINATES_NAME);

  // Set cell sizes
  cell_size_ = 0;
  cell_sizes_.resize(attribute_num_+1);
  for(int i=0; i<= attribute_num_; ++i) {
    cell_sizes_[i] = compute_cell_size(i);
    if(cell_sizes_[i] == VAR_SIZE)
      cell_size_ = VAR_SIZE;
    if(cell_size_ != VAR_SIZE)
      cell_size_ += cell_sizes_[i]; 
  }

  if(coords_ != NULL)
    free(coords_);
  coords_ = malloc(cell_sizes_[attribute_num_]);

  compute_hilbert_cell_bits();
  if(tile_extents_.size() != 0) { // Only for regular tiles
    compute_hilbert_tile_bits();
    compute_tile_id_offsets();
  }
}

/* 
 * The format of array_schema_str is the following (single line, no '\n' 
 * characters involved):
 * 
 * array_name,attribute_num,attribute_1,...,attribute_{attribute_num},
 * dim_num,dim_1,...,dim_{dim_num},
 * dim_domain_1_low,dim_domain_1_high,...,
 * dim_domain_{dim_num}_low,dim_domain_{dim_num}_high
 * type_1,...,type_{attribute_num+1}
 * tile_extents_1,...,tile_extents_{dim_num},
 * cell_order,tile_order,capacity,consolidation_step
 *
 * If one of the items is omitted (e.g., tile_order), then this CSV field
 * should contain '*' (e.g., it should be ...,cell_order,*,capacity,...). 
 * 
 * It returns 0 on success. On error, it prints a message on stderr and
 * returns -1. 
 */                                                            
int ArraySchema::deserialize_csv(std::string array_schema_str) {
  // Create a CSVLine object to parse array_schema_str
  CSVLine array_schema_csv(&array_schema_str[0]);

  // Tempory variable to get a CSV value as a string
  std::string s; 

  // Set array name
  if(!(array_schema_csv >> s)) {
    std::cerr << ERROR_MSG_HEADER  
              << " Array name not provided.\n";
    return -1;
  }
  if(set_array_name(s))
    return -1;

  // Set attribute names
  // ----- retrieve number of attributes
  if(!(array_schema_csv >> s)) {
    std::cerr << ERROR_MSG_HEADER  
              << " Number of attributes not provided.\n";
    return -1;
  }
  int attribute_num;
  if(!is_positive_integer(s.c_str())) { 
    std::cerr << ERROR_MSG_HEADER  
              << " The nunber of attributes must be a positive integer.\n";
    return -1;
  }
  sscanf(s.c_str(), "%d", &attribute_num); 
  // ----- retrieve attribute names
  std::vector<std::string> attribute_names;
  for(int i=0; i<attribute_num; ++i) {
    if(!(array_schema_csv >> s)) {
      std::cerr << ERROR_MSG_HEADER  
                << " The nunber of attribute names does not match the"
                << " provided number of attributes.\n";
      return -1;
    }
    attribute_names.push_back(s);
  }

  // ----- set attribute names
  if(set_attribute_names(attribute_names))
    return -1;

 // Set dimension names
  // ----- retrieve number of dimensions
  if(!(array_schema_csv >> s)) {
    std::cerr << ERROR_MSG_HEADER  
              << " Number of dimensions not provided.\n";
    return -1;
  }
  int dim_num;
  if(!is_positive_integer(s.c_str())) { 
    std::cerr << ERROR_MSG_HEADER  
              << " The nunber of dimensions must be a positive integer.\n";
    return -1;
  }
  sscanf(s.c_str(), "%d", &dim_num); 
  // ----- retrieve dimension names
  std::vector<std::string> dim_names;
  for(int i=0; i<dim_num; ++i) {
    if(!(array_schema_csv >> s)) {
      std::cerr << ERROR_MSG_HEADER  
                << " The nunber of dimension names does not match the"
                << " provided number of dimensions.\n";
      return -1;
    }
    dim_names.push_back(s);
  }
  // ----- set dimension names
  if(set_dim_names(dim_names))
    return -1;

  // Set dimension domains
  // ----- retrieve dimension domains
  std::vector<std::pair<double,double> > dim_domains;
  double lower, upper; 
  std::string lower_str, upper_str; 
  for(int i=0; i<dim_num; ++i) {
    if(!(array_schema_csv >> lower_str)) {
      std::cerr << ERROR_MSG_HEADER  
                << " The nunber of domain bounds does not match the"
                << " provided number of dimensions.\n";
      return -1;
    }
    if(!(array_schema_csv >> upper_str)) {
      std::cerr << ERROR_MSG_HEADER  
                << " The nunber of domain bounds does not match the"
                << " provided number of dimensions.\n";
      return -1;
    }
    if(!is_real(lower_str.c_str()) || !is_real(upper_str.c_str())) {
      std::cerr << ERROR_MSG_HEADER  
                << " The domain bounds must be real numbers.\n";
      return -1;
    } else {
      sscanf(lower_str.c_str(), "%lf", &lower);
      sscanf(upper_str.c_str(), "%lf", &upper);
      dim_domains.push_back(std::pair<double, double>(lower, upper));
    }
  }
  // ----- set dimension domains
  if(set_dim_domains(dim_domains))
    return -1;

  // Set types, val num, types sizes and cell sizes 
  // ----- retrieve attribute types
  std::string type_val_num;  // Example format: 'int' or 'int:4'
  std::string type;
  std::vector<int> val_num;
  std::vector<const std::type_info*> types;
  int num;
  for(int i=0; i<attribute_num; ++i) { 
    if(!(array_schema_csv >> type_val_num)) {
      std::cerr << ERROR_MSG_HEADER  
                << " The nunber of types does not match the number of"
                << " attributes.\n";
      return -1;
    }

    char* token = strtok(&type_val_num[0], ":"); // Type extraction
    type = token;    
    token = strtok(NULL, ":"); // Number of attribute values per cell extraction
    if(token == NULL) { // Missing number of attribute values per cell
      val_num.push_back(1);
    } else {
      // Process next token
      if(!strcmp(token, "var")) { // Variable-sized cell
        val_num.push_back(VAR_SIZE);
      } else if(!is_positive_integer(token)) { 
        // Invalid number of attribute values
        std::cerr << ERROR_MSG_HEADER  
                  << " The number of attribute values per cell must be a"
                  << " positive integer.\n";
        return -1;
      } else { // Valid number of attribute values per cell
        sscanf(token, "%d", &num);
        val_num.push_back(num);
      }

      // No other token should follow
      token = strtok(NULL, ":");
      if(token != NULL) {
        std::cerr << ERROR_MSG_HEADER  
                  << " Redundant arguments in definition of cell type.\n";
        return -1;
      }
    }

    if(type == "char") {
      types.push_back(&typeid(char));
    } else if(type == "int") {
      types.push_back(&typeid(int));
    } else if(type == "int64") {
      types.push_back(&typeid(int64_t));
    } else if(type == "float") {
      types.push_back(&typeid(float));
    } else if(type == "double") {
      types.push_back(&typeid(double));
    } else {
      std::cerr << ERROR_MSG_HEADER  
                << " Invalid attribute type '" << type << "'.\n";
      return -1;
    }
  } 
  // ----- Retrieve coordinate type
  if(!(array_schema_csv >> type)) {
    std::cerr << ERROR_MSG_HEADER  
              << " The nunber of types does not match the number of"
              << " attributes.\n";
    return -1;
  }
  if(type == "int") {
    types.push_back(&typeid(int));
  } else if(type == "int64") {
    types.push_back(&typeid(int64_t));
  } else if(type == "float") {
    types.push_back(&typeid(float));
  } else if(type == "double") {
    types.push_back(&typeid(double));
  } else {
      std::cerr << ERROR_MSG_HEADER  
                << " Invalid coordinates type '" << type << "'\n.";
    return -1;
  }
  // ----- Set types
  if(set_types(types))
    return -1;
  // ----- Set val_num
  if(set_val_num(val_num))
    return -1;

  // Set tile extents
  // ----- retrieve the first tile extent (could be "*")
  std::vector<double> tile_extents;
  double tile_extent;
  if(!(array_schema_csv >> s)) {
    std::cerr << ERROR_MSG_HEADER  
              << " No tile extents provided."
              << " Put '*' to specify iregular tiles.\n";
    return -1;
  }

  if(s != "*") {
    if(!is_real(s.c_str())) {
      std::cerr << ERROR_MSG_HEADER  
                << " The tile extents must be real numbers.\n";
      return -1;
    }
    sscanf(s.c_str(), "%lf", &tile_extent); 
    tile_extents.push_back(tile_extent);
    // ----- retrieve the rest dim_num-1 tile extents
    for(int i=0; i<dim_num-1; ++i) { 
      if(!(array_schema_csv >> s)) {
        std::cerr << ERROR_MSG_HEADER  
                  << " The nunber of tile extents does not match the number of"
                  << " dimensions.\n";
        return -1;
      }
      sscanf(s.c_str(), "%lf", &tile_extent); 
      tile_extents.push_back(tile_extent);
    } 
  }
  // ----- set tile extents
  if(set_tile_extents(tile_extents))
    return -1;

  // Get cell order
  // ----- retrieve cell order
  if(!(array_schema_csv >> s)) {
    std::cerr << ERROR_MSG_HEADER  
              << " No cell order provided."
              << " Put '*' to specify default cell order.\n";
    return -1;
  }
  // ----- check cell order
  CellOrder cell_order;
  if(s == "*") {
    cell_order = AS_CELL_ORDER;
  } else if(s == "row-major") {
    cell_order = CO_ROW_MAJOR;
  } else if(s == "column-major") {
    cell_order = CO_COLUMN_MAJOR;
  } else if(s == "hilbert") {
    cell_order = CO_HILBERT;
  } else {
    std::cerr << ERROR_MSG_HEADER  
              << " Invalid cell order '" << s << "'.\n";
    return -1;
  }
  // ----- set cell order
  if(set_cell_order(cell_order))
    return -1;

  // Get tile order
  // ----- retrieve tile order
  if(!(array_schema_csv >> s)) {
    std::cerr << ERROR_MSG_HEADER  
              << " No tile order provided."
              << " Put '*' to specify default tile order.\n";
    return -1;
  }
  // ----- check cell order
  TileOrder tile_order;
  if(s == "*") {
    if(tile_extents_.size() == 0) // Irregular tiles
      tile_order = TO_NONE;
    else 
      tile_order = AS_TILE_ORDER;
  } else if(s == "row-major") {
    tile_order = TO_ROW_MAJOR;
  } else if(s == "column-major") {
    tile_order = TO_COLUMN_MAJOR;
  } else if(s == "hilbert") {
    tile_order = TO_HILBERT;
  } else {
    std::cerr << ERROR_MSG_HEADER  
              << " Invalid tile order '" << s << "'.\n";
    return -1;
  }
  // ----- set cell order
  if(set_tile_order(tile_order))
    return -1;

  // Get capacity
  // ----- retrieve capacity
  int64_t capacity;
  if(!(array_schema_csv >> s)) {
    std::cerr << ERROR_MSG_HEADER  
              << " No capacity provided."
              << " Put '*' to specify default capacity.\n";
    return -1;
  }

  if(s != "*") {
    if(!is_positive_integer(s.c_str())) {
      std::cerr << ERROR_MSG_HEADER  
                << " The capacity must be a positive integer.\n";
      return -1;
    }
    sscanf(s.c_str(), "%lld", &capacity); 
  } else {
    capacity = AS_CAPACITY;
  }
  // ----- set capacity
  if(set_capacity(capacity))
    return -1;

  // Get consolidation step
  int consolidation_step;
  if(!(array_schema_csv >> s)) {
    std::cerr << ERROR_MSG_HEADER  
              << " No consolidation step provided."
              << " Put '*' to specify default consolidation step.\n";
    return -1;
  }

  if(s != "*") {
    if(!is_positive_integer(s.c_str())) {
      std::cerr << ERROR_MSG_HEADER  
                << " The consolidation step must be a positive integer.\n";
      return -1;
    }
    sscanf(s.c_str(), "%d", &consolidation_step); 
  } else {
    consolidation_step = AS_CONSOLIDATION_STEP;
  }

  // ----- set consolidation step
  if(set_consolidation_step(consolidation_step))
    return -1;

  // Set compression
  std::vector<CompressionType> compression;
  for(int i=0; i<=attribute_num; ++i)
    compression.push_back(NONE);
  set_compression(compression);

  return 0;
}

int ArraySchema::set_array_name(const std::string& array_name) {
  // Check the array name for validity
  if(!is_valid_name(array_name.c_str())) {
    std::cerr << ERROR_MSG_HEADER  
              << " '" << array_name << "' is not a valid array name."
              << " The array name can contain only alphanumerics and '_'.\n";
    return -1;
  }

  // Set array name
  array_name_ = array_name;

  return 0;
}

int ArraySchema::set_attribute_names(
    const std::vector<std::string>& attribute_names) {
  attribute_num_ = attribute_names.size();

  for(size_t i=0; i<attribute_num_; ++i) {
    if(!is_valid_name(attribute_names[i].c_str())) {
      std::cerr << ERROR_MSG_HEADER 
                << " '" << attribute_names[i] 
                << "' is not a valid attribute name."
                " An attribute name can contain only alphanumerics and '_'.\n";
      return -1;
    }
  }

  // Check for duplicate attribute names
  if(duplicates(attribute_names)) {
    std::cerr << ERROR_MSG_HEADER
              << " Duplicate attribute names provided.\n";
    return -1;
  }

  // Check if a dimension has the same name as an attribute 
  if(intersect(attribute_names, dim_names_)) {
    std::cerr << ERROR_MSG_HEADER
              << " An attribute name cannot be the same as a dimension name.\n";
    return -1;
  }

  // All tests passed - set attribute names
  attribute_names_ = attribute_names;
  // Append extra coordinates name
  attribute_names_.push_back(AS_COORDINATES_NAME);

  return 0;
}

int ArraySchema::set_capacity(int64_t capacity) {
  // Check capacity
  if(capacity <= 0) {
    std::cerr << ERROR_MSG_HEADER
              << " The capacity must be a positive integer.\n";
    return -1;
  }
 
  // Set capacity
  capacity_ = capacity;

  return 0;
}

int ArraySchema::set_cell_order(CellOrder cell_order) {
  // Check cell order
  if(cell_order != CO_ROW_MAJOR && 
     cell_order != CO_COLUMN_MAJOR &&
     cell_order != CO_HILBERT) {
    std::cerr << ERROR_MSG_HEADER
              << " Invalid cell order.\n";
    return -1;
  }

  // Set cell order
  cell_order_ = cell_order;
  
  return 0;
}

int ArraySchema::set_compression(
    const std::vector<CompressionType>& compression) {
  // Check number of compression types
  if(compression.size() != attribute_num_+1) {
    std::cerr << ERROR_MSG_HEADER
              << " The nunber of compression types does not match the"
              << " number of attributes.\n";
    return -1;
  }
 
  // Set compression
  compression_ = compression;

  return 0;
}

int ArraySchema::set_consolidation_step(int consolidation_step) {
  // Check consolidation_step
  if(consolidation_step <= 0) {
    std::cerr << ERROR_MSG_HEADER
              << " The consolidation step must be a positive integer.\n";
    return -1;
  }
 
  // Set capacity
  consolidation_step_ = consolidation_step;

  return 0;
}

int ArraySchema::set_dim_domains(
    const std::vector<std::pair<double, double> >& dim_domains) {
  if(dim_domains.size() != dim_num_) {
    std::cerr << ERROR_MSG_HEADER
              << " The nunber of domain bounds does not match the"
              << " provided number of dimensions.\n";
    return -1;
  }

  for(int i=0; i<dim_num_; ++i) {
    if(dim_domains[i].first > dim_domains[i].second) {
      std::cerr << ERROR_MSG_HEADER
                << " A lower domain bound cannot be larger than its"
                << " corresponding upper.";
      return -1;
    }
  }

  // Set the dimension domains
  dim_domains_ = dim_domains;

  // Calculate necessary information for computing hilbert ids 
  compute_hilbert_cell_bits();

  return 0;
}

int ArraySchema::set_dim_names(
    const std::vector<std::string>& dim_names) {
  dim_num_ = dim_names.size();

  for(size_t i=0; i<dim_num_; ++i) {
    if(!is_valid_name(dim_names[i].c_str())) {
      std::cerr << ERROR_MSG_HEADER 
                << " '" << dim_names[i] 
                << "' is not a valid dimension name."
                " A dimension name can contain only alphanumerics and '_'.\n";
      return -1;
    }
  }

  // Check for duplicate dimension names
  if(duplicates(dim_names)) {
    std::cerr << ERROR_MSG_HEADER
              << " Duplicate dimension names provided.\n";
    return -1;
  }

  // Check if a dimension has the same name as an attribute 
  if(intersect(attribute_names_, dim_names)) {
    std::cerr << ERROR_MSG_HEADER
              << " A dimension name cannot be the same as an attribute name.\n";
    return -1;
  }

  // All tests passed - set dimension names
  dim_names_ = dim_names;

  return 0;
}

int ArraySchema::set_tile_extents(
    const std::vector<double>& tile_extents) {
  // Check on dimension domains first
  if(dim_domains_.size() == 0) {
    std::cerr << ERROR_MSG_HEADER
              << " The dimension domains must be set before setting the tile"
              << " extents.\n";
    return -1;
  }

  // Case of irregular tiles 
  if(tile_extents.size() == 0) { 
    tile_extents_ = tile_extents;
    compute_hilbert_cell_bits();
    return 0;
  }

  // Check number of tile_extents
  if(tile_extents.size() != 0 && tile_extents.size() != dim_num_) {
    std::cerr << ERROR_MSG_HEADER
              << " The nunber of tile extents does not match the number of"
              << " dimensions.\n";
    return -1;
  }

  assert(tile_extents.size() == dim_domains_.size());

  // Check soundness of tile extents
  for(int i=0; i<dim_num_; ++i) {
    if(tile_extents[i] > dim_domains_[i].second - dim_domains_[i].first + 1) {
      std::cerr << ERROR_MSG_HEADER
                << " Tile extent exceeds its corresponding domain range.\n";
      return-1;
    }
  }

  // Set the tile extents
  tile_extents_ = tile_extents; 

  // Calculate necessary info for computing hilbert ids 
  compute_hilbert_cell_bits();
  compute_hilbert_tile_bits();
  compute_tile_id_offsets();

  return 0;
}

int ArraySchema::set_tile_order(TileOrder tile_order) {
  // Check tile order
  if(tile_order != TO_ROW_MAJOR && 
     tile_order != TO_COLUMN_MAJOR &&
     tile_order != TO_HILBERT &&
     tile_order != TO_NONE) {
    std::cerr << ERROR_MSG_HEADER
              << " Invalid tile order.\n";
    return -1;
  }

  // Set tile order
  tile_order_ = tile_order;
  
  return 0;
}

int ArraySchema::set_types(
    const std::vector<const std::type_info*>& types) {
  // Check number of types
  if(types.size() != attribute_num_ + 1) {
    std::cerr << ERROR_MSG_HEADER
              << " The nunber of types does not match the number of"
              << " attributes.\n";
    return -1;
  }

  // Check the attribute types
  for(int i=0; i<attribute_num_; ++i) {
    if(types[i] != &typeid(char) && types[i] != &typeid(int) &&
       types[i] != &typeid(int64_t) && types[i] != &typeid(float) &&
       types[i] != &typeid(double)) {
      std::cerr << ERROR_MSG_HEADER << " Invalid attribute type.\n";
      return -1;
    }
  } 
  // Check the attribute types
  if(types[attribute_num_] != &typeid(int) &&
     types[attribute_num_] != &typeid(int64_t) && 
     types[attribute_num_] != &typeid(float) &&
     types[attribute_num_] != &typeid(double)) {
    std::cerr << ERROR_MSG_HEADER << " Invalid coordinate type.\n";
    return -1;
  }

  // Set types
  types_ = types;

  // Set type sizes
  type_sizes_.resize(attribute_num_ + 1);
  for(int i=0; i<=attribute_num_; ++i)
    type_sizes_[i] = compute_type_size(i);

  return 0;
}


int ArraySchema::set_val_num(const std::vector<int>& val_num) {
  // Check number of val num
  if(val_num.size() != attribute_num_) {
    std::cerr << ERROR_MSG_HEADER
              << " The nunber of attribute values per cell does not match"
              << " number of attributes.\n";
    return -1;
  }

  // Set val num
  val_num_ = val_num;

  // Set cell sizes
  cell_size_ = 0;
  cell_sizes_.resize(attribute_num_+1);
  for(int i=0; i<= attribute_num_; ++i) {
    cell_sizes_[i] = compute_cell_size(i);
    if(cell_sizes_[i] == VAR_SIZE)
      cell_size_ = VAR_SIZE;
    if(cell_size_ != VAR_SIZE)
      cell_size_ += cell_sizes_[i]; 
  }

  if(coords_ != NULL)
    free(coords_);
  coords_ = malloc(cell_sizes_[attribute_num_]);

  return 0;
}

/******************************************************
************************ MISC *************************
******************************************************/

int64_t ArraySchema::cell_id_hilbert(const void* coords) const {
  if(*(types_[attribute_num_]) == typeid(int))
    return cell_id_hilbert(static_cast<const int*>(coords));  
  else if(*(types_[attribute_num_]) == typeid(int64_t))
    return cell_id_hilbert(static_cast<const int64_t*>(coords));  
  else if(*(types_[attribute_num_]) == typeid(float))
    return cell_id_hilbert(static_cast<const float*>(coords));  
  else if(*(types_[attribute_num_]) == typeid(double))
    return cell_id_hilbert(static_cast<const double*>(coords));  
}

template<typename T>
int64_t ArraySchema::cell_id_hilbert(const T* coordinates) const {
  assert(*types_[attribute_num_] == typeid(T));
#ifndef NDEBUG
  for(int i=0; i<dim_num_; ++i) 
    assert(coordinates[i] >= dim_domains_[i].first &&
           coordinates[i] <= dim_domains_[i].second);
#endif 

  bool regular = (tile_extents_.size() != 0);

  HilbertCurve *hc = new HilbertCurve();
  int *coord = new int[dim_num_];
 
  if(regular) {
    for(int i = 0; i < dim_num_; ++i) 
      coord[i] = int(coordinates[i]) % int(tile_extents_[i]);
  } else { 
    for(int i = 0; i < dim_num_; ++i) 
      coord[i] = int(coordinates[i]);
  }

  int64_t cell_ID = hc->AxestoLine(coord, hilbert_cell_bits_, dim_num_);	

  delete hc;
  delete [] coord;
	
  return cell_ID;
}

ArraySchema* ArraySchema::clone(const std::string& array_name) const {
  ArraySchema* array_schema = new ArraySchema();
  *array_schema = *this;
  array_schema->array_name_ = array_name; // Input array name

  return array_schema;
}

ArraySchema* ArraySchema::clone(const std::string& array_name,
                                const std::vector<int>& attribute_ids) const {
  assert(valid_attribute_ids(attribute_ids));

  ArraySchema* array_schema = new ArraySchema();
  *array_schema = *this;

  // Change array name
  array_schema->array_name_ = array_name; // Input array name

  // Change attribute names
  array_schema->attribute_names_.clear();
  for(int i=0; i<attribute_ids.size(); ++i) 
    array_schema->attribute_names_.push_back(attribute_name(attribute_ids[i]));
  // Name for the extra coordinate attribute
  array_schema->attribute_names_.push_back(AS_COORDINATES_NAME);
 
  // Change attribute_num_
  array_schema->attribute_num_ = attribute_ids.size();

  // Change cell sizes
  array_schema->cell_sizes_.clear();
  for(int i=0; i<attribute_ids.size(); ++i) 
    array_schema->cell_sizes_.push_back(cell_sizes_[attribute_ids[i]]);
  array_schema->cell_sizes_.push_back(cell_sizes_.back());
 
  // Change types
  array_schema->types_.clear();
  for(int i=0; i<attribute_ids.size(); ++i) 
    array_schema->types_.push_back(types_[attribute_ids[i]]);
  array_schema->types_.push_back(types_.back());

  // Change type sizes
  array_schema->type_sizes_.clear();
  for(int i=0; i<attribute_ids.size(); ++i) 
    array_schema->type_sizes_.push_back(type_sizes_[attribute_ids[i]]);
  array_schema->type_sizes_.push_back(type_sizes_.back());

  // Change val num
  array_schema->val_num_.clear();
  for(int i=0; i<attribute_ids.size(); ++i) 
    array_schema->val_num_.push_back(val_num_[attribute_ids[i]]);
  array_schema->val_num_.push_back(val_num_.back());

  // Change compression
  array_schema->compression_.clear();
  for(int i=0; i<attribute_ids.size(); ++i) 
    array_schema->compression_.push_back(compression_[attribute_ids[i]]);
  array_schema->compression_.push_back(compression_.back());
 
  return array_schema;
}

ArraySchema* ArraySchema::clone(
    const std::string& array_name, CellOrder cell_order) const {
  ArraySchema* array_schema = new ArraySchema();
  *array_schema = *this;
  array_schema->array_name_ = array_name; // Input array name
  array_schema->cell_order_ = cell_order;  // Input cell order

  return array_schema;
}

ArraySchema* ArraySchema::clone(int64_t capacity) const {
  ArraySchema* array_schema = new ArraySchema();
  *array_schema = *this;
  array_schema->capacity_ = capacity; // Input capacity

  return array_schema;
}

ArraySchema ArraySchema::create_join_result_schema(
    const ArraySchema& array_schema_A, 
    const ArraySchema& array_schema_B,
    const std::string& result_array_name) {
  // Attribute names
  std::vector<std::string> join_attribute_names;
  join_attribute_names.insert(join_attribute_names.end(),
                              array_schema_A.attribute_names_.begin(),
                              --array_schema_A.attribute_names_.end());
  std::set<std::string> attribute_names_A_set(
                            array_schema_A.attribute_names_.begin(),
                            --array_schema_A.attribute_names_.end());
  join_attribute_names.insert(join_attribute_names.end(),
                              array_schema_B.attribute_names_.begin(),
                              --array_schema_B.attribute_names_.end());
  for(int i=array_schema_A.attribute_num_; 
      i<join_attribute_names.size(); ++i) {
    if(attribute_names_A_set.find(join_attribute_names[i]) != 
       attribute_names_A_set.end())
      join_attribute_names[i] += "_2";
  }

  // Types
  std::vector<const std::type_info*> join_types;
  join_types.insert(join_types.end(),
                    array_schema_A.types_.begin(),
                    --array_schema_A.types_.end());
  join_types.insert(join_types.end(),
                    array_schema_B.types_.begin(),
                    array_schema_B.types_.end());

  // Number of values per attribute
  std::vector<int> join_val_num;
  join_val_num.insert(join_val_num.end(),
                      array_schema_A.val_num_.begin(),
                      array_schema_A.val_num_.end());
  join_val_num.insert(join_val_num.end(),
                      array_schema_B.val_num_.begin(),
                      array_schema_B.val_num_.end());

  // Irregular tiles
  if(array_schema_A.has_irregular_tiles())
    return ArraySchema(result_array_name, join_attribute_names, 
                       array_schema_A.dim_names_, 
                       array_schema_A.dim_domains_, 
                       join_types, join_val_num,
                       array_schema_A.cell_order_, 
                       array_schema_A.capacity_,
                       array_schema_A.consolidation_step_);
  else // Regular tiles
    return ArraySchema(result_array_name, join_attribute_names, 
                       array_schema_A.dim_names_,
                       array_schema_A.dim_domains_, 
                       join_types, join_val_num,
                       array_schema_A.tile_extents_,
                       array_schema_A.cell_order_,
                       array_schema_A.tile_order_,
                       array_schema_A.consolidation_step_);
} 

// --- Cell format ---
// coordinates, cell_size, 
//	attribute#1_value#1, ...            (fixed-sized attribute)
// 	val_num, attribute#2_value#1,...,   (variable-sized attribute)
void ArraySchema::csv_line_to_cell(
    CSVLine& csv_line,
    void*& cell,
    size_t& cell_size) const {
// TODO: Error messages
  size_t offset = 0;

  // Append coordinates
  append_coordinates(csv_line, cell, cell_size, offset); 
  size_t coords_size = offset; 

  // Make space for cell size 
  if(cell_size_ == VAR_SIZE) 
    offset += sizeof(size_t);

  // Append attribute values
  append_attributes(csv_line, cell, cell_size, offset);

  if(cell_size_ == VAR_SIZE) 
    memcpy(static_cast<char*>(cell) + coords_size, &offset, sizeof(size_t));
}

bool ArraySchema::has_irregular_tiles() const {
  return (tile_extents_.size() == 0);
}

bool ArraySchema::has_regular_tiles() const {
  return (tile_extents_.size() != 0);
}

std::pair<bool,std::string> ArraySchema::join_compatible(
    const ArraySchema& array_schema_A,
    const ArraySchema& array_schema_B) {
  // Tile type 
  if(array_schema_A.has_regular_tiles() != array_schema_B.has_regular_tiles())
    return std::pair<bool,std::string>(false,"Tile type mismatch.");

  // Number of dimensions
  if(array_schema_A.dim_num_ != array_schema_B.dim_num_)
    return std::pair<bool,std::string>(false,"Dimension number mismatch.");

  // Cell type of dimensions
  if(*(array_schema_A.types_[array_schema_A.attribute_num_]) != 
     *(array_schema_B.types_[array_schema_B.attribute_num_]))
    return std::pair<bool,std::string>(false,"Dimension type mismatch.");

  // Domains
  for(int i=0; i<array_schema_A.dim_num_; ++i)
    if(array_schema_A.dim_domains_[i].first != 
       array_schema_B.dim_domains_[i].first || 
       array_schema_A.dim_domains_[i].second != 
       array_schema_B.dim_domains_[i].second)
      return std::pair<bool,std::string>(false,"Domain mismatch.");

  // Orders
  if(array_schema_A.tile_order_ != array_schema_B.tile_order_)
    return std::pair<bool,std::string>(false,"Tile order mismatch.");
  if(array_schema_A.cell_order_ != array_schema_B.cell_order_)
    return std::pair<bool,std::string>(false,"Cell order mismatch.");

  // Tile extents
  for(int i=0; i<array_schema_A.tile_extents_.size(); ++i)
    if(array_schema_A.tile_extents_[i] != array_schema_B.tile_extents_[i])
      return std::pair<bool,std::string>(false,"Tile extent mismatch.");

  return std::pair<bool,std::string>(true,"");
}

bool ArraySchema::precedes(const void* coords_A,
                           const void* coords_B) const {
  if(*types_[attribute_num_] == typeid(int)) {
    return precedes(static_cast<const int*>(coords_A), 
                    static_cast<const int*>(coords_B));
  } else if(*types_[attribute_num_] == typeid(int64_t)) {
    return precedes(static_cast<const int64_t*>(coords_A), 
                    static_cast<const int64_t*>(coords_B));
  } else if(*types_[attribute_num_] == typeid(float)) {
    return precedes(static_cast<const float*>(coords_A), 
                    static_cast<const float*>(coords_B));
  } else if(*types_[attribute_num_] == typeid(double)) {
    return precedes(static_cast<const double*>(coords_A), 
                    static_cast<const double*>(coords_B));
  } else {
    assert(0); // The code must never reach this point
  }
}

template<class T>
bool ArraySchema::precedes(const T* coords_A,
                           const T* coords_B) const {
  assert(*types_[attribute_num_] == typeid(T));

  // ROW_MAJOR order
  if(cell_order_ == CO_ROW_MAJOR) {
    for(int i=0; i<dim_num_; ++i) {
      if(coords_A[i] < coords_B[i])
        return true;
      else if(coords_A[i] > coords_B[i])
        return false;
      // else coordinate is equal - check the next dimension
    }
    return false; // Coordinates are equal
  // COLUMN_MAJOR order
  } else if(cell_order_ == CO_COLUMN_MAJOR) {
    for(int i=dim_num_-1; i>=0; i--) {
      if(coords_A[i] < coords_B[i])
        return true;
      else if(coords_A[i] > coords_B[i])
        return false;
      // else coordinate is equal - check the next dimension
    }
    return false; // Coordinates are equal
  // HILBERT order
  } else if(cell_order_ == CO_HILBERT) {
    int64_t cell_id_A = cell_id_hilbert(coords_A);
    int64_t cell_id_B = cell_id_hilbert(coords_B);
    if(cell_id_A < cell_id_B) {
      return true;
    } else if (cell_id_A > cell_id_B) {
      return false;
    } else { // (cell_id_A == cell_id_B): check ROW_MAJOR order
      for(int i=0; i<dim_num_; ++i) {
        if(coords_A[i] < coords_B[i])
          return true;
        else if(coords_A[i] > coords_B[i])
          return false;
        // else coordinate is equal - check the next dimension
      }
      return false; // Coordinates are equal
    }
  } else { // it should never reach this point
    assert(0);
  }
}

void ArraySchema::print() const {
  std::cout << "Array name: " << array_name_ << "\n";
  if(has_regular_tiles()) {
    std::cout << "Tile order: ";
    if(tile_order_ == TO_COLUMN_MAJOR)
      std::cout << "COLUMN MAJOR\n";
    else if(tile_order_ == TO_HILBERT)
      std::cout << "HILBERT\n";
    else if(tile_order_ == TO_ROW_MAJOR)
      std::cout << "ROW_MAJOR\n";
    else if(tile_order_ == TO_NONE)
      std::cout << "NONE\n";
  }
  std::cout << "Cell order: ";
  if(cell_order_ == CO_COLUMN_MAJOR)
    std::cout << "COLUMN MAJOR\n";
  else if(cell_order_ == CO_HILBERT)
    std::cout << "HILBERT\n";
  else if(cell_order_ == CO_ROW_MAJOR)
    std::cout << "ROW_MAJOR\n";
  else if(cell_order_ == CO_NONE)
    std::cout << "NONE\n";

  if(has_irregular_tiles())
    std::cout << "Capacity: " << capacity_ << "\n";
  std::cout << "Consolidation step: " << consolidation_step_ << "\n";

  std::cout << "Attribute num: " << attribute_num_ << "\n";
  std::cout << "Attribute names:\n";
  for(int i=0; i<attribute_num_; ++i)
    std::cout << "\t" << attribute_names_[i] << "\n";
  std::cout << "Dimension num: " << dim_num_ << "\n";
  std::cout << "Dimension names:\n";
  for(int i=0; i<dim_num_; ++i)
    std::cout << "\t" << dim_names_[i] << "\n";
  std::cout << "Dimension domains:\n";
  for(int i=0; i<dim_num_; ++i)
    std::cout << "\t" << dim_names_[i] << ": [" << dim_domains_[i].first << ","
                       << dim_domains_[i].second << "]\n";
  std::cout << (has_regular_tiles() ? "Regular" : "Irregular") << " tiles\n";
  if(has_regular_tiles()) {
    std::cout << "Tile extents:\n";
    for(int i=0; i<dim_num_; ++i)
      std::cout << "\t" << dim_names_[i] << ": " << tile_extents_[i] << "\n";
  }

  std::cout << "Cell types:\n";
  for(int i=0; i<attribute_num_; ++i) {
    if(*types_[i] == typeid(char)) {
      std::cout << "\t" << attribute_names_[i] << ": char[";
    } else if(*types_[i] == typeid(int)) {
      std::cout << "\t" << attribute_names_[i] << ": int[";
    } else if(*types_[i] == typeid(int64_t)) {
      std::cout << "\t" << attribute_names_[i] << ": int64[";
    } else if(*types_[i] == typeid(float)) {
      std::cout << "\t" << attribute_names_[i] << ": float[";
    } else if(*types_[i] == typeid(double)) {
      std::cout << "\t" << attribute_names_[i] << ": double[";
    }
    if(val_num_[i] == VAR_SIZE)
      std::cout << "var]\n";
    else
      std::cout << val_num_[i] << "]\n";
  }
  if(*types_[attribute_num_] == typeid(int))
    std::cout << "\tCoordinates: int\n";
  else if(*types_[attribute_num_] == typeid(int64_t))
    std::cout << "\tCoordinates: int64\n";
  else if(*types_[attribute_num_] == typeid(float))
    std::cout << "\tCoordinates: float\n";
  else if(*types_[attribute_num_] == typeid(double))
    std::cout << "\tCoordinates: double\n";

  std::cout << "Cell sizes (in bytes):\n";
  for(int i=0; i<=attribute_num_; ++i) {
    std::cout << "\t" << ((i==attribute_num_) ? "Coordinates"  
                                              : attribute_names_[i]) 
                      << ": ";
    if(cell_sizes_[i] == VAR_SIZE)
      std::cout << "var\n"; 
    else
      std::cout << cell_sizes_[i] << "\n"; 
  }

  std::cout << "Compression types:\n";
  for(int i=0; i<attribute_num_; ++i)
    if(compression_[i] == RLE)
      std::cout << "\t" << attribute_names_[i] << ": RLE\n";
    else if(compression_[i] == ZIP)
      std::cout << "\t" << attribute_names_[i] << ": ZIP\n";
    else if(compression_[i] == LZ)
      std::cout << "\t" << attribute_names_[i] << ": LZ\n";
    else if(compression_[i] == NONE)
      std::cout << "\t" << attribute_names_[i] << ": NONE\n";
  if(compression_[attribute_num_] == RLE)
    std::cout << "\tCoordinates: RLE\n";
  else if(compression_[attribute_num_] == ZIP)
    std::cout << "\tCoordinates: ZIP\n";
  else if(compression_[attribute_num_] == LZ)
    std::cout << "\tCoordinates: LZ\n";
  else if(compression_[attribute_num_] == NONE)
    std::cout << "\tCoordinates: NONE\n";
}

bool ArraySchema::succeeds(const void* coords_A,
                           const void* coords_B) const {
  if(*types_[attribute_num_] == typeid(int)) {
    return succeeds(static_cast<const int*>(coords_A), 
                    static_cast<const int*>(coords_B));
  } else if(*types_[attribute_num_] == typeid(int64_t)) {
    return succeeds(static_cast<const int64_t*>(coords_A), 
                    static_cast<const int64_t*>(coords_B));
  } else if(*types_[attribute_num_] == typeid(float)) {
    return succeeds(static_cast<const float*>(coords_A), 
                    static_cast<const float*>(coords_B));
  } else if(*types_[attribute_num_] == typeid(double)) {
    return succeeds(static_cast<const double*>(coords_A), 
                    static_cast<const double*>(coords_B));
  } else {
    assert(0); // The code must never reach this point
  }
}

template<class T>
bool ArraySchema::succeeds(const T* coords_A,
                           const T* coords_B) const {
  assert(*types_[attribute_num_] == typeid(T));

  // ROW_MAJOR order
  if(cell_order_ == CO_ROW_MAJOR) {
    for(int i=0; i<dim_num_; ++i) {
      if(coords_A[i] > coords_B[i])
        return true;
      else if(coords_A[i] < coords_B[i])
        return false;
      // else coordinate is equal - check the next dimension
    }
    return false; // Coordinates are equal
  // COLUMN_MAJOR order
  } else if(cell_order_ == CO_COLUMN_MAJOR) {
    for(int i=dim_num_-1; i>=0; --i) {
      if(coords_A[i] > coords_B[i])
        return true;
      else if(coords_A[i] < coords_B[i])
        return false;
      // else coordinate is equal - check the next dimension
    }
    return false; // Coordinates are equal
  // HILBERT order
  } else if(cell_order_ == CO_HILBERT) {
    int64_t cell_id_A = cell_id_hilbert(coords_A);
    int64_t cell_id_B = cell_id_hilbert(coords_B);
    if(cell_id_A > cell_id_B) {
      return true;
    } else if (cell_id_A < cell_id_B) {
      return false;
    } else { // (cell_id_A == cell_id_B): check ROW_MAJOR order
      for(int i=0; i<dim_num_; ++i) {
        if(coords_A[i] > coords_B[i])
          return true;
        else if(coords_A[i] < coords_B[i])
          return false;
        // else coordinate is equal - check the next dimension
      }
      return false; // Coordinates are equal
    }
  } else { // it should never reach this point
    assert(0);
  }
}

template<class T>
int64_t ArraySchema::tile_id(const T* coords) const {
  // Applicable only to regular tiles
  assert(tile_extents_.size() != 0);

  if(tile_order_ == ArraySchema::TO_ROW_MAJOR)
    return tile_id_row_major<T>(coords);
  else if(tile_order_ == ArraySchema::TO_COLUMN_MAJOR)
    return tile_id_column_major<T>(coords);
  else if(tile_order_ == ArraySchema::TO_HILBERT)
    return tile_id_hilbert<T>(coords);
}

int64_t ArraySchema::tile_id_column_major(const void* coords) const {
  if(*(types_[attribute_num_]) == typeid(int))
    return tile_id_column_major(static_cast<const int*>(coords));  
  else if(*(types_[attribute_num_]) == typeid(int64_t))
    return tile_id_column_major(static_cast<const int64_t*>(coords));  
  else if(*(types_[attribute_num_]) == typeid(float))
    return tile_id_column_major(static_cast<const float*>(coords));  
  else if(*(types_[attribute_num_]) == typeid(double))
    return tile_id_column_major(static_cast<const double*>(coords));  
}

template<typename T>
int64_t ArraySchema::tile_id_column_major(const T* coords) const {
  assert(check_on_tile_id_request(coords));
 
  int64_t tile_ID = 0;
  int64_t partition_id;
  for(int i = 0; i < dim_num_; ++i) {
    partition_id = floor(coords[i] / tile_extents_[i]);
    tile_ID += partition_id * tile_id_offsets_column_major_[i];
  }	

  return tile_ID;
}

int64_t ArraySchema::tile_id_hilbert(const void* coords) const {
  if(*(types_[attribute_num_]) == typeid(int))
    return tile_id_hilbert(static_cast<const int*>(coords));  
  else if(*(types_[attribute_num_]) == typeid(int64_t))
    return tile_id_hilbert(static_cast<const int64_t*>(coords));  
  else if(*(types_[attribute_num_]) == typeid(float))
    return tile_id_hilbert(static_cast<const float*>(coords));  
  else if(*(types_[attribute_num_]) == typeid(double))
    return tile_id_hilbert(static_cast<const double*>(coords));  
}

template<typename T>
int64_t ArraySchema::tile_id_hilbert(const T* coords) const {
  assert(check_on_tile_id_request(coords));
  	
  HilbertCurve *hc = new HilbertCurve();
  int *int_coords = new int[dim_num_];

  for(int i = 0; i < dim_num_; ++i) 
    int_coords[i] = static_cast<int>(coords[i]/tile_extents_[i]);

  int64_t tile_ID = hc->AxestoLine(int_coords, hilbert_tile_bits_, dim_num_);	

  delete hc;
  delete [] int_coords;

  return tile_ID;
}

int64_t ArraySchema::tile_id_row_major(const void* coords) const {
  if(*(types_[attribute_num_]) == typeid(int))
    return tile_id_row_major(static_cast<const int*>(coords));  
  else if(*(types_[attribute_num_]) == typeid(int64_t))
    return tile_id_row_major(static_cast<const int64_t*>(coords));  
  else if(*(types_[attribute_num_]) == typeid(float))
    return tile_id_row_major(static_cast<const float*>(coords));  
  else if(*(types_[attribute_num_]) == typeid(double))
    return tile_id_row_major(static_cast<const double*>(coords));  
}

template<typename T>
int64_t ArraySchema::tile_id_row_major(const T* coords) const {
  check_on_tile_id_request(coords);
 
  int64_t tile_ID = 0;
  int64_t partition_id;
  for(int i = 0; i < dim_num_; ++i) {
    partition_id = floor(coords[i] / tile_extents_[i]);
    tile_ID += partition_id * tile_id_offsets_row_major_[i];
  }	

  return tile_ID;
}

const ArraySchema* ArraySchema::transpose(
    const std::string& new_array_name) const {
  assert(dim_num_ == 2);

  // Copy array schema and give new name
  ArraySchema* new_array_schema = new ArraySchema();
  *new_array_schema = *this;
  new_array_schema->array_name_ = new_array_name;
  
  // Transpose the dim domains
  std::vector<std::pair<double, double> > new_dim_domains;
  new_dim_domains.push_back(dim_domains_[1]);
  new_dim_domains.push_back(dim_domains_[0]);
  new_array_schema->dim_domains_ = new_dim_domains;

  return new_array_schema;
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

void ArraySchema::append_attributes(
    CSVLine& csv_line, void*& cell, size_t& cell_size, size_t& offset) const {
  for(int i=0; i<attribute_num_; ++i) {
    if(types_[i] == &typeid(char))
      append_attribute<char>(csv_line, val_num_[i], cell, cell_size, offset);
    else if(types_[i] == &typeid(int))
      append_attribute<int>(csv_line, val_num_[i], cell, cell_size, offset);
    else if(types_[i] == &typeid(int64_t))
      append_attribute<int64_t>(csv_line, val_num_[i], cell, cell_size, offset);
    else if(types_[i] == &typeid(float))
      append_attribute<float>(csv_line, val_num_[i], cell, cell_size, offset);
    else if(types_[i] == &typeid(double))
      append_attribute<double>(csv_line, val_num_[i], cell, cell_size, offset);
  }
}

template<class T>
void ArraySchema::append_attribute(
    CSVLine& csv_line, int val_num, void*& cell, 
    size_t& cell_size, size_t& offset) const {
  T v;
  char* c_cell = static_cast<char*>(cell);

  if(val_num != VAR_SIZE) { // Fixed-sized attribute 
    while(offset + val_num*sizeof(T) > cell_size) {
      expand_buffer(cell, cell_size);
      c_cell = static_cast<char*>(cell);
      cell_size *= 2;
    } 
    for(int i=0; i<val_num; ++i) {
      csv_line >> v;
      memcpy(c_cell + offset, &v, sizeof(T));
      offset += sizeof(T);
    }
  } else {                  // Variable-sized attribute 
    if(typeid(T) == typeid(char)) {
      std::string s;
      csv_line >> s;
      int s_size = s.size();

      while(offset + s_size + sizeof(int) > cell_size) {
        expand_buffer(cell, cell_size);
        c_cell = static_cast<char*>(cell);
        cell_size *= 2;
      } 

      memcpy(c_cell + offset, &s_size, sizeof(int));
      offset += sizeof(int);
      memcpy(c_cell + offset, s.c_str(), s.size());
      offset += s.size(); 
    } else {
      int num;
      csv_line >> num;

      while(offset + num*sizeof(T) + sizeof(int) > cell_size) {
        expand_buffer(cell, cell_size);
        c_cell = static_cast<char*>(cell);
        cell_size *= 2;
      } 

      memcpy(c_cell + offset, &num, sizeof(int));
      offset += sizeof(int);
      for(int i=0; i<num; ++i) {
        csv_line >> v;
        memcpy(c_cell + offset, &v, sizeof(T));
        offset += sizeof(T);
      }
    }
  }
}

void ArraySchema::append_coordinates(
    CSVLine& csv_line, void*& cell, size_t& cell_size, size_t& offset) const {
  if(types_[attribute_num_] == &typeid(int))
    append_coordinates<int>(csv_line, cell, cell_size, offset);
  if(types_[attribute_num_] == &typeid(int64_t))
    append_coordinates<int64_t>(csv_line, cell, cell_size, offset);
  if(types_[attribute_num_] == &typeid(float))
    append_coordinates<float>(csv_line, cell, cell_size, offset);
  if(types_[attribute_num_] == &typeid(double))
    append_coordinates<double>(csv_line, cell, cell_size, offset);
}

template<class T>
void ArraySchema::append_coordinates(
    CSVLine& csv_line, void*& cell, size_t& cell_size, size_t& offset) const {
//  T* coords = new T[dim_num_];

  T* coords = static_cast<T*>(coords_);
  size_t coords_size = dim_num_*sizeof(T);

  while(offset + coords_size > cell_size) {
    expand_buffer(cell, cell_size);
    cell_size *= 2;
  } 

  for(int i=0; i<dim_num_; ++i) {
    if(!(csv_line >> coords[i])) {
      delete [] coords;
    }
  }

  memcpy(cell, coords, coords_size);
  offset += coords_size;

//  delete [] coords;
}

// --- Cell format ---
// coordinates, cell_size, 
//	attribute#1_value#1, ...            (fixed-sized attribute)
// 	val_num, attribute#2_value#1,...,   (variable-sized attribute)
ssize_t ArraySchema::calculate_cell_size(CSVLine& csv_line) const {
  // Initialize cell size - it will be updated below
  ssize_t cell_size = coords_size() + sizeof(size_t);
  // Skip the coordinates in the CSV line
  csv_line += dim_num_; 
 
  for(int i=0; i<attribute_num_; ++i) {
    if(cell_sizes_[i] != VAR_SIZE) { // Fixed-sized attribute
      cell_size += cell_sizes_[i];
      csv_line += val_num_[i];
    } else {                                     // Variable-sized attribute
      if(types_[i] == &typeid(char)) {
        cell_size += sizeof(int) + strlen(*csv_line)*sizeof(char);
        ++csv_line;
      } else {
        int num;

        if(!(csv_line >> num))
          return -1; // Error

        if(types_[i] == &typeid(int)) 
          cell_size += sizeof(int) + num*sizeof(int);
        else if(types_[i] == &typeid(float)) 
          cell_size += sizeof(int) + num*sizeof(float);
        else if(types_[i] == &typeid(double)) 
          cell_size += sizeof(int) + num*sizeof(double);

        csv_line += num;
      }
    }
  }

  // Reset the position of the CSV line to the beginning
  csv_line.reset();

  return cell_size;
}

template<typename T>
bool ArraySchema::check_on_tile_id_request(const T* coords) const {
  if(has_irregular_tiles() || *types_[attribute_num_] != typeid(T))
    return false; 

  for(int i=0; i<dim_num_; ++i) 
    if(coords[i] < dim_domains_[i].first ||
       coords[i] > dim_domains_[i].second)
      return false;

  return true;
}

size_t ArraySchema::compute_cell_size(int i) const {
  assert(i>= 0 && i <= attribute_num_);

  // Variable-sized cell
  if(i<attribute_num_ && val_num_[i] == VAR_SIZE)
    return VAR_SIZE;

  // Fixed-sized cell
  size_t size;
  
  // Attributes
  if(i < attribute_num_) {
    if(types_[i] == &typeid(char))
      size = val_num_[i] * sizeof(char);
    else if(types_[i] == &typeid(int))
      size = val_num_[i] * sizeof(int);
    else if(types_[i] == &typeid(int64_t))
      size = val_num_[i] * sizeof(int64_t);
    else if(types_[i] == &typeid(float))
      size = val_num_[i] * sizeof(float);
    else if(types_[i] == &typeid(double))
      size = val_num_[i] * sizeof(double);
  } else { // Coordinates
    if(types_[i] == &typeid(int))
      size = dim_num_ * sizeof(int);
    else if(types_[i] == &typeid(int64_t))
      size = dim_num_ * sizeof(int64_t);
    else if(types_[i] == &typeid(float))
      size = dim_num_ * sizeof(float);
    else if(types_[i] == &typeid(double))
      size = dim_num_ * sizeof(double);
  }

  return size; 
}

void ArraySchema::compute_hilbert_cell_bits() {
  double max_domain_range = 0;
  double domain_range;
  bool regular = (tile_extents_.size() != 0);

  for(int i = 0; i < dim_num_; ++i) { 
    if(regular) // Regular tiles: ids are calculated within a tile
      domain_range = tile_extents_[i];
    else        // Irregular tiles: ids are calculated in the entire domain
      domain_range = dim_domains_[i].second - dim_domains_[i].first + 1;
    if(max_domain_range < domain_range)
      max_domain_range = domain_range;
  }

  hilbert_cell_bits_ = ceil(log2(static_cast<int64_t>(max_domain_range+0.5)));
}

void ArraySchema::compute_hilbert_tile_bits() {
  assert(has_regular_tiles());

  double max_domain_range = 0;
  double domain_range;

  for(int i = 0; i < dim_num_; ++i) {       
    domain_range = (dim_domains_[i].second - dim_domains_[i].first + 1) /
                    tile_extents_[i];  
    if(max_domain_range < domain_range)
      max_domain_range = domain_range;
  }

  hilbert_tile_bits_ = ceil(log2(static_cast<int64_t>(max_domain_range+0.5)));
}

void ArraySchema::compute_tile_id_offsets() {
  assert(has_regular_tiles());
  
  double domain_range;
  int64_t partition_num; // Number of partitions on some axis
  int64_t offset_row = 1;
  int64_t offset_column = 1;

  tile_id_offsets_row_major_.push_back(offset_row);	
  tile_id_offsets_column_major_.push_back(offset_column);	

  for(int i=0; i<dim_num_-1 ; ++i) {
    // Row major
    domain_range = dim_domains_[i].second - dim_domains_[i].first + 1;
    partition_num = ceil(domain_range / tile_extents_[i]);
    offset_row *= partition_num;
    tile_id_offsets_row_major_.push_back(offset_row);
   
    // Column major
    domain_range = dim_domains_[dim_num_-1-i].second - 
                   dim_domains_[dim_num_-1-i].first + 1;
    partition_num = ceil(domain_range / tile_extents_[dim_num_-1-i]);
    offset_column *= partition_num;
    tile_id_offsets_column_major_.push_back(offset_column);
  }
 
  // For column major only 
  std::reverse(tile_id_offsets_column_major_.begin(), 
               tile_id_offsets_column_major_.end());
}

size_t ArraySchema::compute_type_size(int i) const {
  assert(i>= 0 && i <= attribute_num_);

  if(types_[i] == &typeid(char))
    return sizeof(char);
  else if(types_[i] == &typeid(int))
    return sizeof(int);
  else if(types_[i] == &typeid(int64_t))
    return sizeof(int64_t);
  else if(types_[i] == &typeid(float))
    return sizeof(float);
  else if(types_[i] == &typeid(double))
    return sizeof(double);
}

std::pair<ArraySchema::AttributeIds, ArraySchema::AttributeIds>
ArraySchema::get_attribute_ids(
    const std::set<std::string>& attribute_names) const {
  // Get the ids of the attribute names corresponding to the input names
  std::vector<int> attribute_ids;
  std::set<std::string>::const_iterator attr_it = 
      attribute_names.begin();
  std::set<std::string>::const_iterator attr_it_end = 
      attribute_names.end();
  for(; attr_it != attr_it_end; ++attr_it) 
    attribute_ids.push_back(attribute_id(*attr_it));
  std::sort(attribute_ids.begin(), attribute_ids.end());
  int input_attribute_num = attribute_ids.size();

  // Find the ids of the attributes NOT corresponding to the input names
  std::vector<int> non_attribute_ids;
  for(int j=0; j<attribute_ids[0]; ++j)
    non_attribute_ids.push_back(j);
  for(int i=1; i<input_attribute_num; ++i) {
    for(int j=attribute_ids[i-1]+1; j<attribute_ids[i]; ++j)
      non_attribute_ids.push_back(j);
  }
  for(int j=attribute_ids[input_attribute_num-1] + 1; 
      j<=attribute_num_; ++j)
    non_attribute_ids.push_back(j);

  return std::pair<AttributeIds, AttributeIds>(attribute_ids, 
                                               non_attribute_ids);
}

int ArraySchema::get_attribute_ids(
    const std::vector<std::string>& attribute_names,
    std::vector<int>& attribute_ids) const {
  // Initialization
  attribute_ids.clear();

  // If "hide attributes" is selected, the return list should be empty
  if(attribute_names.size() == 0 || attribute_names[0] != "__hide") {
    // Default ids in case the name list is empty
    if(attribute_names.size() == 0) {
      attribute_ids.resize(attribute_num_);
      for(int i=0; i<attribute_num_; ++i)
        attribute_ids[i] = i;
    } else { // Ids retrieved from the name list
      attribute_ids.resize(attribute_names.size());
      for(int i=0; i<attribute_names.size(); ++i) {
        attribute_ids[i] = attribute_id(attribute_names[i]);
        if(attribute_ids[i] == -1) {
// TODO          err_msg = std::string("Invalid attribute name ") + 
//                    attribute_names[i] + ".";
          return -1;
        }
      }
    }
  }

  return 0;

}

bool ArraySchema::valid_attribute_ids(
    const std::vector<int>& attribute_ids) const {
  for(int i=0; i<attribute_ids.size(); ++i) {
    if(attribute_ids[i] < 0 || attribute_ids[i] > attribute_num_) 
      return false;
  }

  return true;
}

// Explicit template instantiations
template bool ArraySchema::precedes<int>(
    const int* coords_A, const int* coords_B) const;
template bool ArraySchema::precedes<int64_t>(
    const int64_t* coords_A, const int64_t* coords_B) const;
template bool ArraySchema::precedes<float>(
    const float* coords_A, const float* coords_B) const;
template bool ArraySchema::precedes<double>(
    const double* coords_A, const double* coords_B) const;

template bool ArraySchema::succeeds<int>(
    const int* coords_A, const int* coords_B) const;
template bool ArraySchema::succeeds<int64_t>(
    const int64_t* coords_A, const int64_t* coords_B) const;
template bool ArraySchema::succeeds<float>(
    const float* coords_A, const float* coords_B) const;
template bool ArraySchema::succeeds<double>(
    const double* coords_A, const double* coords_B) const;

template int64_t ArraySchema::cell_id_hilbert<int>(
    const int* coords) const;
template int64_t ArraySchema::cell_id_hilbert<int64_t>(
    const int64_t* coords) const;
template int64_t ArraySchema::cell_id_hilbert<float>(
    const float* coords) const;
template int64_t ArraySchema::cell_id_hilbert<double>(
    const double* coords) const;

template int64_t ArraySchema::tile_id<int>(
    const int* coords) const;
template int64_t ArraySchema::tile_id<int64_t>(
    const int64_t* coords) const;
template int64_t ArraySchema::tile_id<float>(
    const float* coords) const;
template int64_t ArraySchema::tile_id<double>(
    const double* coords) const;

template int64_t ArraySchema::tile_id_row_major<int>(
    const int* coords) const;
template int64_t ArraySchema::tile_id_row_major<int64_t>(
    const int64_t* coords) const;
template int64_t ArraySchema::tile_id_row_major<float>(
    const float* coords) const;
template int64_t ArraySchema::tile_id_row_major<double>(
    const double* coords) const;

template int64_t ArraySchema::tile_id_column_major<int>(
    const int* coords) const;
template int64_t ArraySchema::tile_id_column_major<int64_t>(
    const int64_t* coords) const;
template int64_t ArraySchema::tile_id_column_major<float>(
    const float* coords) const;
template int64_t ArraySchema::tile_id_column_major<double>(
    const double* coords) const;

template int64_t ArraySchema::tile_id_hilbert<int>(
    const int* coordinates) const;
template int64_t ArraySchema::tile_id_hilbert<int64_t>(
    const int64_t* coords) const;
template int64_t ArraySchema::tile_id_hilbert<float>(
    const float* coords) const;
template int64_t ArraySchema::tile_id_hilbert<double>(
    const double* coords) const;
