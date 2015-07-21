/**
 * @file   cell.cc
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
 * This file implements the class Cell.
 */

#include "cell.h"
#include "utils.h"
#include <iostream>
#include <string.h>
#include <assert.h>

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

Cell::Cell(
    const ArraySchema* array_schema, 
    int id_num,
    bool random_access) 
    : array_schema_(array_schema), 
      random_access_(random_access),
      id_num_(id_num) {
  assert(id_num >= 0);

  // Set attribute_ids_
  attribute_ids_= array_schema_->attribute_ids();

  // Set cell_size_ and var_size_
  cell_size_ = array_schema_->cell_size() + id_num * sizeof(int64_t);
  var_size_ = array_schema_->var_size();
}

Cell::Cell(
    const void* cell, 
    const ArraySchema* array_schema, 
    int id_num, 
    bool random_access) 
    : array_schema_(array_schema), 
      id_num_(id_num), 
      random_access_(random_access) {
  assert(id_num >= 0);

  // Set attribute_ids_
  attribute_ids_= array_schema_->attribute_ids();

  // Set cell
  set_cell(cell);

  // Set cell_size_ and var_size_
  cell_size_ = array_schema_->cell_size() + id_num * sizeof(int64_t);
  var_size_ = array_schema_->var_size();
}

Cell::Cell(
    const ArraySchema* array_schema,
    const std::vector<int>& attribute_ids,
    int id_num,
    bool random_access) 
    : array_schema_(array_schema), 
      id_num_(id_num),
      random_access_(random_access) { 
  assert(id_num >= 0);

  // For easy reference
  int attribute_num = array_schema->attribute_num(); 

  // Checks
  assert(attribute_ids.size() != 0);
  assert(no_duplicates(attribute_ids));
  assert(attribute_ids.back() == attribute_num);

  // Set attribute_ids_
  attribute_ids_= attribute_ids;

  // Set cell_size_ and var_size_
  cell_size_ = array_schema_->cell_size(attribute_ids_) + 
               id_num * sizeof(int64_t);
  var_size_ = array_schema_->var_size();
}

Cell::~Cell() {
}

/******************************************************
********************* ACCESSORS ***********************
******************************************************/

const ArraySchema* Cell::array_schema() const {
  return array_schema_;
} 

int Cell::attribute_id(int i) const {
  return attribute_ids_[i];
}

int Cell::attribute_num() const {
  return attribute_ids_.size();
}

CellConstAttrIterator Cell::begin() const {
  return CellConstAttrIterator(this, 0); 
}

const void* Cell::cell() const {
  return cell_;
}

ssize_t Cell::cell_size() const {
  if(cell_ == NULL)
    return 0;

  if(!var_size_) {
    return cell_size_;
  } else {
    size_t cell_size;
    memcpy(&cell_size, 
           id_num_ * sizeof(int64_t) + array_schema_->coords_size() +
           static_cast<const char*>(cell_), 
           sizeof(size_t));
    return cell_size + id_num_ * sizeof(int64_t); 
  }
}

template<class T>
CSVLine Cell::csv_line(
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids) const {
  // For easy reference
  int dim_num = array_schema_->dim_num();
  int attribute_num = array_schema_->attribute_num();

  // Initialization
  CSVLine csv_line;

  // Append ids
  for(int i=0; i<id_num_; ++i) 
    csv_line << id(i); 

  // Append coordinates
  const T* coords = (*this)[attribute_num];
  for(int i=0; i<dim_ids.size(); ++i) {
    assert(dim_ids[i] >= 0 && dim_ids[i] < array_schema_->dim_num());
    csv_line << coords[dim_ids[i]];
  }

  // Append attribute values
  for(int i=0; i<attribute_ids.size(); ++i) {
    assert(attribute_ids[i] >= 0 && attribute_ids[i] < attribute_num);

    const std::type_info* attr_type = array_schema_->type(attribute_ids[i]);

    if(attr_type == &typeid(char)) {
      if(var_size(attribute_ids[i])) // Special case for strings 
        append_string(attribute_ids[i], csv_line);
      else
        append_attribute<char>(attribute_ids[i], csv_line);
    } else if(attr_type == &typeid(int)) {
      append_attribute<int>(attribute_ids[i], csv_line);
    } else if(attr_type == &typeid(int64_t)) {
      append_attribute<int64_t>(attribute_ids[i], csv_line);
    } else if(attr_type == &typeid(float)) {
      append_attribute<float>(attribute_ids[i], csv_line);
    } else if(attr_type == &typeid(double)) {
      append_attribute<double>(attribute_ids[i], csv_line);
    } 
  }

  return csv_line;
}

int64_t Cell::id(int i) const {
  assert(i >= 0 && i < id_num_); 

  int64_t id_r;
  memcpy(&id_r, 
         static_cast<const char*>(cell_) + i * sizeof(int64_t), 
         sizeof(int64_t)); 

  return id_r;
}

size_t Cell::ids_size() const {
  return id_num_ * sizeof(int64_t);
}

int Cell::val_num(int attribute_id) const {
  int val_num = array_schema_->val_num(attribute_id);
 
  // Variable-lengthed attribute 
  if(val_num == VAR_SIZE) {
    std::map<int, int>::const_iterator it = val_num_.find(attribute_id);
    assert(it != val_num_.end());
    val_num = it->second;
  }

  return val_num;
}

bool Cell::var_size() const {
  return var_size_;
}

bool Cell::var_size(int attribute_id) const {
  return array_schema_->cell_size(attribute_id) == VAR_SIZE;
}

/******************************************************
********************* MUTATORS ************************
******************************************************/

void Cell::set_cell(const void* cell) {
  cell_ = cell;

  // Initialization
  if(cell_ != NULL && random_access_) {
    init_val_num();
    init_attribute_offsets();
  }
}

/******************************************************
********************* OPERATORS ***********************
******************************************************/

TypeConverter Cell::operator[](int attribute_id) const {
  // Soundness check
  std::map<int, size_t>::const_iterator it = 
      attribute_offsets_.find(attribute_id);
  assert(it != attribute_offsets_.end());

  // Return the start of the attribute value
  return TypeConverter(static_cast<const char*>(cell_) + it->second);
}

/******************************************************
***************** PRIVATE METHODS *********************
******************************************************/

template<class T>
void Cell::append_attribute(int attribute_id, CSVLine& csv_line) const {
  int val_num = this->val_num(attribute_id);
  bool var_size = this->var_size(attribute_id);

  // Number of values for the case of variable-sized attribute
  if(typeid(T) != typeid(char) && var_size) 
    csv_line << val_num;

  // Append attribute values
  const T* v;
  if(!var_size) {
    v = (*this)[attribute_id];
  } else { // Skip the val_num
    const void* temp = 
        static_cast<const char*>((*this)[attribute_id]) + sizeof(int);
    v = static_cast<const T*>(temp);
  }

  for(int i=0; i<val_num; ++i) {
    if(is_null(v[i]))
      csv_line << NULL_VALUE;
    else if(is_del(v[i]))
      csv_line << DEL_VALUE;
    else 
      csv_line << v[i];
  }
}

void Cell::append_string(int attribute_id, CSVLine& csv_line) const {
  int val_num = this->val_num(attribute_id);
  std::string v;
  v.resize(val_num);

  const char* s = static_cast<const char*>((*this)[attribute_id]) + sizeof(int);

  v.assign(s, val_num);

  if(is_null(v[0]))
    csv_line << NULL_VALUE;
  else if(is_del(v[0]))
    csv_line << DEL_VALUE;
  else 
    csv_line << v;
}

void Cell::init_attribute_offsets() {
  attribute_offsets_.clear();

  // Coordinates
  attribute_offsets_[array_schema_->attribute_num()] = 
      id_num_ * sizeof(int64_t); 

  // Attributes
  CellConstAttrIterator attr_it = begin();
  for(; !attr_it.end(); ++attr_it) 
    attribute_offsets_[attr_it.attribute_id()] = attr_it.offset();
}

void Cell::init_val_num() {
  val_num_.clear();

  // For easy reference
  int attribute_num = array_schema_->attribute_num();

  if(var_size_) {
    size_t offset = id_num_ * sizeof(int64_t) + sizeof(size_t) + 
                    array_schema_->cell_size(attribute_num); 
    
    // For all attributes (excluding coordinates)
    int val_num;
    for(int i=0; i<attribute_ids_.size()-1; ++i) {
      val_num = array_schema_->val_num(attribute_ids_[i]);
      if(val_num == VAR_SIZE) {
        memcpy(&val_num, static_cast<const char*>(cell_) + offset, sizeof(int));
        offset += sizeof(int); 
      }
      offset += val_num * array_schema_->type_size(attribute_ids_[i]); 
      val_num_[attribute_ids_[i]] = val_num;
    }
  }
}     

// Explicit template instantiations
template CSVLine Cell::csv_line<int>(
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids) const;
template CSVLine Cell::csv_line<int64_t>(
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids) const;
template CSVLine Cell::csv_line<float>(
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids) const;
template CSVLine Cell::csv_line<double>(
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids) const;
