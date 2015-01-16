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
#include "hilbert_curve.h"
#include "assert.h"
#include <string.h>
#include <math.h>
#include <algorithm>
#include <iostream>
#include <set>

/******************************************************
************ CONSTRUCTORS & DESTRUCTORS ***************
******************************************************/

ArraySchema::ArraySchema(
    const std::string& array_name,
    const std::vector<std::string>& attribute_names,
    const std::vector<std::string>& dim_names,
    const std::vector<std::pair<double, double> >& dim_domains,
    const std::vector<const std::type_info*>& types,
    Order order,
    uint64_t capacity) {
  assert(attribute_names.size() > 0);
  assert(dim_names.size() > 0);
  assert(attribute_names.size()+1 == types.size());
  assert(dim_names.size() == dim_domains.size());
  assert(capacity != 0);
#ifndef NDEBUG
  for(unsigned int i=0; i<dim_domains.size(); i++) 
    assert(dim_domains[i].first <= dim_domains[i].second);
#endif
 
  array_name_ = array_name;
  attribute_names_ = attribute_names;
  dim_names_ = dim_names;
  dim_domains_ = dim_domains;
  types_ = types;
  order_ = order;
  capacity_ = capacity;
  dim_num_ = dim_names_.size();
  attribute_num_ = attribute_names_.size();
  // Name for the extra coordinate attribute
  attribute_names_.push_back(AS_COORDINATE_TILE_NAME);

  compute_hilbert_cell_bits();
}

ArraySchema::ArraySchema(
    const std::string& array_name,
    const std::vector<std::string>& attribute_names,
    const std::vector<std::string>& dim_names,
    const std::vector<std::pair<double, double> >& dim_domains,
    const std::vector<const std::type_info*>& types,
    Order order,
    const std::vector<double>& tile_extents,
    uint64_t capacity) {
  assert(attribute_names.size() > 0);
  assert(dim_names.size() > 0);
  assert(attribute_names.size()+1 == types.size());
  assert(dim_names.size() == dim_domains.size());
  assert(dim_names.size() == tile_extents.size());
  assert(capacity != 0);
#ifndef NDEBUG
  for(unsigned int i=0; i<dim_domains.size(); i++) 
    assert(dim_domains[i].first <= dim_domains[i].second);
  for(unsigned int i=0; i<tile_extents.size(); i++) {
    assert(tile_extents[i] != 0);
    assert(tile_extents[i] <= (dim_domains[i].second - dim_domains[i].first + 1));
  }
#endif

  array_name_ = array_name;
  attribute_names_ = attribute_names;
  dim_names_ = dim_names;
  dim_domains_ = dim_domains;
  types_ = types;
  order_ = order;
  capacity_ = capacity;
  tile_extents_ = tile_extents; 
  dim_num_ = dim_names_.size();
  attribute_num_ = attribute_names_.size();
  // Name for the extra coordinate attribute
  attribute_names_.push_back(AS_COORDINATE_TILE_NAME); 

  compute_hilbert_cell_bits();
  compute_hilbert_tile_bits();
  compute_tile_id_offsets();
}

/******************************************************
********************** ACCESSORS **********************
******************************************************/

const std::string& ArraySchema::attribute_name(unsigned int i) const {
  assert(i <= attribute_num_);

  return attribute_names_[i];
}

uint64_t ArraySchema::cell_size(unsigned int i) const {
  assert(i <= attribute_num_);

  uint64_t size;

  if(*types_[i] == typeid(char))
    size = sizeof(char);
  else if(*types_[i] == typeid(int))
    size = sizeof(int);
  else if(*types_[i] == typeid(int64_t))
    size = sizeof(int64_t);
  else if(*types_[i] == typeid(float))
    size = sizeof(float);
  else if(*types_[i] == typeid(double))
    size = sizeof(double);

  // Attribute cell
  if(i < attribute_num_)
    return size; 
  else // (i == attribute_num), i.e., coordinate cell
    return dim_num_ * size;
}

uint64_t ArraySchema::max_cell_size() const {
  uint64_t size, max_size = 0;
  for(unsigned int i=0; i <= attribute_num_; i++) {
    size = cell_size(i);
    if(max_size < size)
      max_size = size;
  }

  return max_size;
}

// FORMAT:
// array_name_size(unsigned int) array_name(string)
// order(unsigned char)
// capacity(uint64_t)
// attribute_num(unsigned int) 
//     attribute_name_size#1(unsigned int) attribute_name#1(string)
//     attribute_name_size#2(unsigned int) attribute_name#2(string) 
//     ...
// dim_num(unsigned int) 
//    dim_name_size#1(unsigned int) dim_name#1(string)
//    dim_name_size#2(unsigned int) dim_name#2(string)
//    ...
// dim_domain#1_low(double) dim_domain#1_high(double)
// dim_domain#2_low(double) dim_domain#2_high(double)
//  ...
// tile_extents_num(unsigned int) 
//     tile_extent#1(double) tile_extent#2(double) ... 
// type#1(unsigned_char) type#2(unsigned char) ... 
std::pair<char*, uint64_t> ArraySchema::serialize() const {
  uint64_t buffer_size = 0;

  // ====== Calculation of buffer_size ======
  // Size for array_name_ 
  buffer_size += sizeof(unsigned int) + array_name_.size();
  // Size for order_ 
  buffer_size += sizeof(unsigned char);
  // Size for capacity_ 
  buffer_size += sizeof(uint64_t);
  // Size for attribute_names_ 
  buffer_size += sizeof(unsigned int);
  for(unsigned int i=0; i<attribute_num_; i++)
    buffer_size += sizeof(unsigned int) + attribute_names_[i].size();
  // Size for dim_names_
  buffer_size += sizeof(unsigned int);
  for(unsigned int i=0; i<dim_num_; i++)
    buffer_size += sizeof(unsigned int) + dim_names_[i].size();
  // Size for dim_domains_
  buffer_size += 2 * dim_num_ * sizeof(double);
  // Size for tile_extents_ 
  // (recall that an array with irregular tiles does not have tile extents)
  buffer_size += sizeof(unsigned int) + tile_extents_.size() * sizeof(double);
  // Size for types_
  buffer_size += (attribute_num_+1) * sizeof(unsigned char);

  char* buffer = new char[buffer_size];

  // ====== Populating the buffer ======
  uint64_t offset = 0;
  // Copy array_name_
  unsigned int array_name_size = array_name_.size();
  assert(offset < buffer_size);
  memcpy(buffer + offset, &array_name_size, sizeof(unsigned int));
  offset += sizeof(unsigned int);
  assert(offset < buffer_size);
  memcpy(buffer + offset, array_name_.c_str(), array_name_size);
  offset += array_name_size;
  // Copu order_
  unsigned char order = order_;
  assert(offset < buffer_size);
  memcpy(buffer + offset, &order, sizeof(unsigned char));
  offset += sizeof(unsigned char);
  // Copy capacity_
  assert(offset < buffer_size);
  memcpy(buffer + offset, &capacity_, sizeof(uint64_t));
  offset += sizeof(uint64_t);
  // Copy attribute_names_
  assert(offset < buffer_size);
  memcpy(buffer + offset, &attribute_num_, sizeof(unsigned int));
  offset += sizeof(unsigned int);
  unsigned int attribute_name_size;
  for(unsigned int i=0; i<attribute_num_; i++) {
    attribute_name_size = attribute_names_[i].size();
    assert(offset < buffer_size);
    memcpy(buffer + offset, &attribute_name_size, sizeof(unsigned int)); 
    offset += sizeof(unsigned int);
    assert(offset < buffer_size);
    memcpy(buffer + offset, attribute_names_[i].c_str(), attribute_name_size); 
    offset += attribute_name_size;
  }
  // Copy dim_names_
  assert(offset < buffer_size);
  memcpy(buffer + offset, &dim_num_, sizeof(unsigned int));
  offset += sizeof(unsigned int);
  unsigned int dim_name_size;
  for(unsigned int i=0; i<dim_num_; i++) {
    dim_name_size = dim_names_[i].size();
    assert(offset < buffer_size);
    memcpy(buffer + offset, &dim_name_size, sizeof(unsigned int)); 
    offset += sizeof(unsigned int);
    assert(offset < buffer_size);
    memcpy(buffer + offset, dim_names_[i].c_str(), dim_name_size); 
    offset += dim_name_size;
  }
  // Copy dim_domains_
  for(unsigned int i=0; i<dim_num_; i++) {
    assert(offset < buffer_size);
    memcpy(buffer + offset, &dim_domains_[i].first, sizeof(double));
    offset += sizeof(double);
    assert(offset < buffer_size);
    memcpy(buffer + offset, &dim_domains_[i].second, sizeof(double));
    offset += sizeof(double);
  } 
  // Copy tile_extents_
  unsigned int tile_extents_num = tile_extents_.size();
  assert(offset < buffer_size);
  memcpy(buffer + offset, &tile_extents_num, sizeof(unsigned int));
  offset += sizeof(unsigned int);
  for(unsigned int i=0; i<tile_extents_num; i++) {
    assert(offset < buffer_size);
    memcpy(buffer + offset, &tile_extents_[i], sizeof(double));
    offset += sizeof(double);
  }
  // Copy types_
  unsigned char type; 
  for(unsigned int i=0; i<=attribute_num_; i++) {
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
    memcpy(buffer + offset, &type, sizeof(unsigned char));
    offset += sizeof(unsigned char);
  }
  assert(offset == buffer_size);

  return std::pair<char*, uint64_t>(buffer, buffer_size);
}

const std::type_info* ArraySchema::type(unsigned int i) const {
  assert(i <= attribute_num_);

  return types_[i];
}

/******************************************************
*********************** MUTATORS **********************
******************************************************/

// FORMAT:
// array_name_size(unsigned int) array_name(string)
// order(unsigned char)
// capacitry(uint64_t)
// attribute_num(unsigned int) 
//     attribute_name_size#1(unsigned int) attribute_name#1(string)
//     attribute_name_size#2(unsigned int) attribute_name#2(string) 
//     ...
// dim_num(unsigned int) 
//    dim_name_size#1(unsigned int) dim_name#1(string)
//    dim_name_size#2(unsigned int) dim_name#2(string)
//    ...
// dim_domain#1_low(double) dim_domain#1_high(double)
// dim_domain#2_low(double) dim_domain#2_high(double)
//  ...
// tile_extents_num(unsigned int) 
//     tile_extent#1(double) tile_extent#2(double) ... 
// type#1(unsigned_char) type#2(unsigned char) ... 
void ArraySchema::deserialize(const char* buffer, uint64_t buffer_size) {
  uint64_t offset = 0;

  // Load array_name_ 
  unsigned int array_name_size;
  assert(offset < buffer_size);
  memcpy(&array_name_size, buffer + offset, sizeof(unsigned int));
  offset += sizeof(unsigned int);
  array_name_.resize(array_name_size);
  assert(offset < buffer_size);
  memcpy(&array_name_[0], buffer + offset, array_name_size);
  offset += array_name_size;
  // Load order
  unsigned char order;
  assert(offset < buffer_size);
  memcpy(&order, buffer + offset, sizeof(unsigned char));
  order_ = static_cast<Order>(order);  
  offset += sizeof(unsigned char);
  // Load capacity
  assert(offset < buffer_size);
  memcpy(&capacity_, buffer + offset, sizeof(uint64_t));
  offset += sizeof(uint64_t);
  // Load attribute_names_
  assert(offset < buffer_size);
  memcpy(&attribute_num_, buffer + offset, sizeof(unsigned int));
  offset += sizeof(unsigned int);
  attribute_names_.resize(attribute_num_);
  unsigned int attribute_name_size;
  for(unsigned int i=0; i<attribute_num_; i++) {
    assert(offset < buffer_size);
    memcpy(&attribute_name_size, buffer+offset, sizeof(unsigned int)); 
    offset += sizeof(unsigned int);
    attribute_names_[i].resize(attribute_name_size);
    assert(offset < buffer_size);
    memcpy(&attribute_names_[i][0], 
           buffer + offset, attribute_name_size);
    offset += attribute_name_size;
  }
  // Load dim_names_
  assert(offset < buffer_size);
  memcpy(&dim_num_, buffer + offset, sizeof(unsigned int));
  offset += sizeof(unsigned int);
  dim_names_.resize(dim_num_);
  unsigned int dim_name_size;
  for(unsigned int i=0; i<dim_num_; i++) {
    assert(offset < buffer_size);
    memcpy(&dim_name_size, buffer + offset, sizeof(unsigned int)); 
    offset += sizeof(unsigned int);
    dim_names_[i].resize(dim_name_size);
    assert(offset < buffer_size);
    memcpy(&dim_names_[i][0], buffer + offset, dim_name_size); 
    offset += dim_name_size;
  }
  // Load dim_domains
  dim_domains_.resize(dim_num_);
  for(unsigned int i=0; i<dim_num_; i++) {
    assert(offset < buffer_size);
    memcpy(&dim_domains_[i].first, buffer + offset, sizeof(double));
    offset += sizeof(double);
    assert(offset < buffer_size);
    memcpy(&dim_domains_[i].second, buffer + offset, sizeof(double));
    offset += sizeof(double);
  } 
  // Load tile_extents_
  unsigned int tile_extents_num;
  assert(offset < buffer_size);
  memcpy(&tile_extents_num, buffer + offset, sizeof(unsigned int));
  offset += sizeof(unsigned int);
  tile_extents_.resize(tile_extents_num);
  for(unsigned int i=0; i<tile_extents_num; i++) {
    assert(offset < buffer_size);
    memcpy(&tile_extents_[i], buffer + offset, sizeof(double));
    offset += sizeof(double);
  }
  // Load types_
  unsigned char type;
  types_.resize(attribute_num_+1); 
  for(unsigned int i=0; i<=attribute_num_; i++) {
    assert(offset < buffer_size);
    memcpy(&type, buffer + offset, sizeof(unsigned char));
    offset += sizeof(unsigned char);
    if(type == CHAR)
      types_[i] = &typeid(char);
    else if(type == INT)
      types_[i] = &typeid(int);
    else if(type == INT64_T)
      types_[i] = &typeid(int64_t);
    else if(type == FLOAT)
      types_[i] = &typeid(float);
    else if(type == DOUBLE)
      types_[i] = &typeid(double);
  }
  assert(offset == buffer_size);
  
  // Extra coordinate attribute
  attribute_names_.push_back(AS_COORDINATE_TILE_NAME);

  compute_hilbert_cell_bits();
  if(tile_extents_.size() != 0) { // Only for regular tiles
    compute_hilbert_tile_bits();
    compute_tile_id_offsets();
  }
}

/******************************************************
************************ MISC *************************
******************************************************/

template<typename T>
uint64_t ArraySchema::cell_id_hilbert(
    const std::vector<T>& coordinates) const {
  assert(coordinates.size() == dim_num_);
#ifndef NDEBUG
  for(unsigned int i=0; i<dim_num_; i++) 
    assert(coordinates[i] >= dim_domains_[i].first &&
           coordinates[i] <= dim_domains_[i].second);
#endif 

  HilbertCurve *hc = new HilbertCurve();
  int *coord = new int[dim_num_];
  
  for(unsigned int i = 0; i < dim_num_; i++) 
    coord[i] = static_cast<int>(coordinates[i]);

  uint64_t cell_ID = hc->AxestoLine(coord, hilbert_cell_bits_, dim_num_);	

  delete hc;
  delete [] coord;
	
  return cell_ID;
}

ArraySchema ArraySchema::clone(const std::string& array_name) const {
  ArraySchema array_schema;

  array_schema.array_name_ = array_name; // Input name
  array_schema.attribute_names_ = attribute_names_;
  array_schema.dim_names_ = dim_names_;
  array_schema.dim_domains_ = dim_domains_;
  array_schema.types_ = types_;
  array_schema.order_ = order_;
  array_schema.capacity_ = capacity_;
  array_schema.tile_extents_ = tile_extents_; 
  array_schema.dim_num_ = dim_num_;
  array_schema.attribute_num_ = attribute_num_;
  array_schema.hilbert_cell_bits_ = hilbert_cell_bits_;
  array_schema.hilbert_tile_bits_ = hilbert_tile_bits_;
  array_schema.tile_id_offsets_column_major_ = tile_id_offsets_column_major_;
  array_schema.tile_id_offsets_row_major_ = tile_id_offsets_row_major_;

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
  for(unsigned int i=array_schema_A.attribute_num_; 
      i<join_attribute_names.size(); i++) {
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

  if(array_schema_A.has_irregular_tiles())
    return ArraySchema(result_array_name, join_attribute_names, 
                       array_schema_A.dim_names_, 
                       array_schema_A.dim_domains_, join_types,
                       array_schema_A.order_, array_schema_A.capacity_);
  else
    return ArraySchema(result_array_name, join_attribute_names, 
                       array_schema_A.dim_names_,
                       array_schema_A.dim_domains_, join_types,
                       array_schema_A.order_, array_schema_A.tile_extents_,
                       array_schema_A.capacity_);
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

  // Type of dimensions
  if(*(array_schema_A.types_[array_schema_A.attribute_num_]) != 
     *(array_schema_B.types_[array_schema_B.attribute_num_]))
    return std::pair<bool,std::string>(false,"Dimension type mismatch.");

  // Domains
  for(unsigned int i=0; i<array_schema_A.dim_num_; i++)
    if(array_schema_A.dim_domains_[i].first != 
       array_schema_B.dim_domains_[i].first || 
       array_schema_A.dim_domains_[i].second != 
       array_schema_B.dim_domains_[i].second)
      return std::pair<bool,std::string>(false,"Domain mismatch.");

  // Order
  if(array_schema_A.order_ != array_schema_B.order_)
    return std::pair<bool,std::string>(false,"Order mismatch.");

  // Tile extents
  for(unsigned int i=0; i<array_schema_A.tile_extents_.size(); i++)
    if(array_schema_A.tile_extents_[i] != array_schema_B.tile_extents_[i])
      return std::pair<bool,std::string>(false,"Tile extent mismatch.");

  return std::pair<bool,std::string>(true,"");
}

template<class T>
bool ArraySchema::precedes(const std::vector<T>& coord_A,
                           const std::vector<T>& coord_B) const {
  assert(coord_A.size() == coord_B.size());
  assert(coord_A.size() == dim_num_);

  // ROW_MAJOR order
  if(has_regular_tiles() || order_ == ROW_MAJOR) {
    for(unsigned int i=0; i<dim_num_; ++i) {
      if(coord_A[i] < coord_B[i])
        return true;
      else if(coord_A[i] > coord_B[i])
        return false;
      // else coordinate is equal - check the next dimension
    }
    return false; // Coordinates are equal
  // COLUMN_MAJOR order
  } else if(order_ == COLUMN_MAJOR) {
    for(int i=dim_num_-1; i>=0; i--) {
      if(coord_A[i] < coord_B[i])
        return true;
      else if(coord_A[i] > coord_B[i])
        return false;
      // else coordinate is equal - check the next dimension
    }
    return false; // Coordinates are equal
  // HILBERT order
  } else if(order_ == HILBERT) {
    uint64_t cell_id_A = cell_id_hilbert(coord_A);
    uint64_t cell_id_B = cell_id_hilbert(coord_B);
    if(cell_id_A < cell_id_B) {
      return true;
    } else if (cell_id_A > cell_id_B) {
      return false;
    } else { // (cell_id_A == cell_id_B): check ROW_MAJOR order
      for(unsigned int i=0; i<dim_num_; ++i) {
        if(coord_A[i] < coord_B[i])
          return true;
        else if(coord_A[i] > coord_B[i])
          return false;
        // else coordinate is equal - check the next dimension
      }
      return false; // Coordinates are equal
    }
  } else { // it should never reach this point
    assert(0);
  }
}

bool ArraySchema::precedes(const Tile::const_iterator& cell_it_A,
                           const Tile::const_iterator& cell_it_B) const {
  // For easy reference
  const Tile* tile_A = cell_it_A.tile();
  const Tile* tile_B = cell_it_B.tile();

  // Works only for coordinates
  assert(tile_A->tile_type() == Tile::COORDINATE);
  assert(tile_B->tile_type() == Tile::COORDINATE);
  assert(tile_A->dim_num() == dim_num_);
  assert(tile_B->dim_num() == dim_num_);

  if(*(types_[attribute_num_]) == typeid(int)) {
    const std::vector<int>& coord_A = *cell_it_A;
    const std::vector<int>& coord_B = *cell_it_B;
    return precedes(coord_A, coord_B);
  } else if(*(types_[attribute_num_]) == typeid(int64_t)) {
    const std::vector<int64_t>& coord_A = *cell_it_A;
    const std::vector<int64_t>& coord_B = *cell_it_B;
    return precedes(coord_A, coord_B);
  } else if(*(types_[attribute_num_]) == typeid(float)) {
    const std::vector<float>& coord_A = *cell_it_A;
    const std::vector<float>& coord_B = *cell_it_B;
    return precedes(coord_A, coord_B);
  } else if(*(types_[attribute_num_]) == typeid(double)) {
    const std::vector<double>& coord_A = *cell_it_A;
    const std::vector<double>& coord_B = *cell_it_B;
    return precedes(coord_A, coord_B);
  }
}

void ArraySchema::print() const {
  std::cout << "Array name: " << array_name_ << "\n";
  std::cout << "Order: ";
  if(order_ == COLUMN_MAJOR)
    std::cout << "COLUMN MAJOR\n";
  else if(order_ == HILBERT)
    std::cout << "HILBERT\n";
  if(order_ == ROW_MAJOR)
    std::cout << "ROW_MAJOR\n";
  std::cout << "Capacity: " << capacity_ << "\n";
  std::cout << "Attribute num: " << attribute_num_ << "\n";
  std::cout << "Attribute names:\n";
  for(unsigned int i=0; i<attribute_num_; i++)
    std::cout << "\t" << attribute_names_[i] << "\n";
  std::cout << "Dimension num: " << dim_num_ << "\n";
  std::cout << "Dimension names:\n";
  for(unsigned int i=0; i<dim_num_; i++)
    std::cout << "\t" << dim_names_[i] << "\n";
  std::cout << "Dimension domains:\n";
  for(unsigned int i=0; i<dim_num_; i++)
    std::cout << "\t[" << dim_domains_[i].first << "," 
                        << dim_domains_[i].second << "]\n";
  std::cout << (has_regular_tiles() ? "Regular" : "Irregular") << " tiles\n";
  if(has_regular_tiles()) {
    std::cout << "Tile extents:\n";
    for(unsigned int i=0; i<dim_num_; i++)
      std::cout << "\t" << tile_extents_[i] << "\n";
  }
  std::cout << "Types:\n";
  for(unsigned int i=0; i<=attribute_num_; i++)
    if(*types_[i] == typeid(char))
      std::cout << "\tchar\n";
    else if(*types_[i] == typeid(int))
      std::cout << "\tint\n";
    else if(*types_[i] == typeid(int64_t))
      std::cout << "\tint64_t\n";
    else if(*types_[i] == typeid(float))
      std::cout << "\tfloat\n";
    else if(*types_[i] == typeid(double))
      std::cout << "\tdouble\n";
}

template<class T>
bool ArraySchema::succeeds(const std::vector<T>& coord_A,
                           const std::vector<T>& coord_B) const {
  assert(coord_A.size() == coord_B.size());
  assert(coord_A.size() == dim_num_);

  // ROW_MAJOR order
  if(has_regular_tiles() || order_ == ROW_MAJOR) {
    for(unsigned int i=0; i<dim_num_; ++i) {
      if(coord_A[i] > coord_B[i])
        return true;
      else if(coord_A[i] < coord_B[i])
        return false;
      // else coordinate is equal - check the next dimension
    }
    return false; // Coordinates are equal
  // COLUMN_MAJOR order
  } else if(order_ == COLUMN_MAJOR) {
    for(int i=dim_num_-1; i>=0; i--) {
      if(coord_A[i] > coord_B[i])
        return true;
      else if(coord_A[i] < coord_B[i])
        return false;
      // else coordinate is equal - check the next dimension
    }
    return false; // Coordinates are equal
  // HILBERT order
  } else if(order_ == HILBERT) {
    uint64_t cell_id_A = cell_id_hilbert(coord_A);
    uint64_t cell_id_B = cell_id_hilbert(coord_B);
    if(cell_id_A > cell_id_B) {
      return true;
    } else if (cell_id_A < cell_id_B) {
      return false;
    } else { // (cell_id_A == cell_id_B): check ROW_MAJOR order
      for(unsigned int i=0; i<dim_num_; ++i) {
        if(coord_A[i] > coord_B[i])
          return true;
        else if(coord_A[i] < coord_B[i])
          return false;
        // else coordinate is equal - check the next dimension
      }
      return false; // Coordinates are equal
    }
  } else { // it should never reach this point
    assert(0);
  }
}

bool ArraySchema::succeeds(const Tile::const_iterator& cell_it_A,
                           const Tile::const_iterator& cell_it_B) const {
  // For easy reference
  const Tile* tile_A = cell_it_A.tile();
  const Tile* tile_B = cell_it_B.tile();

  // Works only for coordinates
  assert(tile_A->tile_type() == Tile::COORDINATE);
  assert(tile_B->tile_type() == Tile::COORDINATE);
  assert(tile_A->dim_num() == dim_num_);
  assert(tile_B->dim_num() == dim_num_);

  if(*(types_[attribute_num_]) == typeid(int)) {
    const std::vector<int>& coord_A = *cell_it_A;
    const std::vector<int>& coord_B = *cell_it_B;
    return succeeds(coord_A, coord_B);
  } else if(*(types_[attribute_num_]) == typeid(int64_t)) {
    const std::vector<int64_t>& coord_A = *cell_it_A;
    const std::vector<int64_t>& coord_B = *cell_it_B;
    return succeeds(coord_A, coord_B);
  } else if(*(types_[attribute_num_]) == typeid(float)) {
    const std::vector<float>& coord_A = *cell_it_A;
    const std::vector<float>& coord_B = *cell_it_B;
    return succeeds(coord_A, coord_B);
  } else if(*(types_[attribute_num_]) == typeid(double)) {
    const std::vector<double>& coord_A = *cell_it_A;
    const std::vector<double>& coord_B = *cell_it_B;
    return succeeds(coord_A, coord_B);
  }
}

template<typename T>
uint64_t ArraySchema::tile_id_column_major(
    const std::vector<T>& coordinates) const {
  assert(check_on_tile_id_request(coordinates));
 
  uint64_t tile_ID = 0;
  uint64_t partition_id;
  for(unsigned int i = 0; i < dim_num_; i++) {
    partition_id = floor(coordinates[i] / tile_extents_[i]);
    tile_ID += partition_id * tile_id_offsets_column_major_[i];
  }	

  return tile_ID;
}

template<typename T>
uint64_t ArraySchema::tile_id_hilbert(const std::vector<T>& coordinates) const {
  assert(check_on_tile_id_request(coordinates));
  	
  HilbertCurve *hc = new HilbertCurve();
  int *coord = new int[dim_num_];

  for(unsigned int i = 0; i < dim_num_; i++) 
    coord[i] = static_cast<int>(coordinates[i]/tile_extents_[i]);

  uint64_t tile_ID = hc->AxestoLine(coord, hilbert_tile_bits_, dim_num_);	

  delete hc;
  delete [] coord;

  return tile_ID;
}

template<typename T>
uint64_t ArraySchema::tile_id_row_major(
    const std::vector<T>& coordinates) const {
  check_on_tile_id_request(coordinates);
 
  uint64_t tile_ID = 0;
  uint64_t partition_id;
  for(unsigned int i = 0; i < dim_num_; i++) {
    partition_id = floor(coordinates[i] / tile_extents_[i]);
    tile_ID += partition_id * tile_id_offsets_row_major_[i];
  }	

  return tile_ID;
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

template<typename T>
bool ArraySchema::check_on_tile_id_request(
    const std::vector<T>& coordinates) const {
  if(has_irregular_tiles() || coordinates.size() != dim_num_)
    return false; 

  for(unsigned int i=0; i<dim_num_; i++) 
    if(coordinates[i] < dim_domains_[i].first ||
       coordinates[i] > dim_domains_[i].second)
      return false;

  return true;
}

void ArraySchema::compute_hilbert_cell_bits() {
  double max_domain_range = 0;
  double domain_range;
  for(unsigned int i = 0; i < dim_num_; i++) {       
    domain_range = dim_domains_[i].second - dim_domains_[i].first + 1;
    if(max_domain_range < domain_range)
      max_domain_range = domain_range;
  }

  hilbert_cell_bits_ = ceil(log2(static_cast<uint64_t>(max_domain_range+0.5)));
}

void ArraySchema::compute_hilbert_tile_bits() {
  assert(has_regular_tiles());

  double max_domain_range = 0;
  double domain_range;
  for(unsigned int i = 0; i < dim_num_; i++) {       
    domain_range = (dim_domains_[i].second - dim_domains_[i].first + 1) /
                    tile_extents_[i];  
    if(max_domain_range < domain_range)
      max_domain_range = domain_range;
  }

  hilbert_tile_bits_ = ceil(log2(static_cast<uint64_t>(max_domain_range+0.5)));
}

void ArraySchema::compute_tile_id_offsets() {
  assert(has_regular_tiles());
  
  double domain_range;
  uint64_t partition_num; // Number of partitions on some axis
  uint64_t offset_row = 1;
  uint64_t offset_column = 1;

  tile_id_offsets_row_major_.push_back(offset_row);	
  tile_id_offsets_column_major_.push_back(offset_column);	

  for(unsigned int i=0; i<dim_num_-1 ; i++) {
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

// Explicit template instantiations
template bool ArraySchema::precedes<int>(
    const std::vector<int>& coord_A,
    const std::vector<int>& coord_B) const;
template bool ArraySchema::precedes<int64_t>(
    const std::vector<int64_t>& coord_A,
    const std::vector<int64_t>& coord_B) const;
template bool ArraySchema::precedes<float>(
    const std::vector<float>& coord_A,
    const std::vector<float>& coord_B) const;
template bool ArraySchema::precedes<double>(
    const std::vector<double>& coord_A,
    const std::vector<double>& coord_B) const;
template bool ArraySchema::succeeds<int>(
    const std::vector<int>& coord_A,
    const std::vector<int>& coord_B) const;
template bool ArraySchema::succeeds<int64_t>(
    const std::vector<int64_t>& coord_A,
    const std::vector<int64_t>& coord_B) const;
template bool ArraySchema::succeeds<float>(
    const std::vector<float>& coord_A,
    const std::vector<float>& coord_B) const;
template bool ArraySchema::succeeds<double>(
    const std::vector<double>& coord_A,
    const std::vector<double>& coord_B) const;
template uint64_t ArraySchema::cell_id_hilbert<int>(
    const std::vector<int>& coordinates) const;
template uint64_t ArraySchema::cell_id_hilbert<int64_t>(
    const std::vector<int64_t>& coordinates) const;
template uint64_t ArraySchema::cell_id_hilbert<float>(
    const std::vector<float>& coordinates) const;
template uint64_t ArraySchema::cell_id_hilbert<double>(
    const std::vector<double>& coordinates) const;
template uint64_t ArraySchema::tile_id_row_major<int>(
    const std::vector<int>& coordinates) const;
template uint64_t ArraySchema::tile_id_row_major<int64_t>(
    const std::vector<int64_t>& coordinates) const;
template uint64_t ArraySchema::tile_id_row_major<float>(
    const std::vector<float>& coordinates) const;
template uint64_t ArraySchema::tile_id_row_major<double>(
    const std::vector<double>& coordinates) const;
template uint64_t ArraySchema::tile_id_column_major<int>(
    const std::vector<int>& coordinates) const;
template uint64_t ArraySchema::tile_id_column_major<int64_t>(
    const std::vector<int64_t>& coordinates) const;
template uint64_t ArraySchema::tile_id_column_major<float>(
    const std::vector<float>& coordinates) const;
template uint64_t ArraySchema::tile_id_column_major<double>(
    const std::vector<double>& coordinates) const;
template uint64_t ArraySchema::tile_id_hilbert<int>(
    const std::vector<int>& coordinates) const;
template uint64_t ArraySchema::tile_id_hilbert<int64_t>(
    const std::vector<int64_t>& coordinates) const;
template uint64_t ArraySchema::tile_id_hilbert<float>(
    const std::vector<float>& coordinates) const;
template uint64_t ArraySchema::tile_id_hilbert<double>(
    const std::vector<double>& coordinates) const;
