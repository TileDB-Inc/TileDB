/*
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
#include "global.h"
#include "utils.h"
#include <cassert>
#include <cstring>
#include <iostream>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#if VERBOSE == 1
#  define PRINT_ERROR(x) std::cerr << "[TileDB] Error: " << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB] Warning: " \
                                     << x << ".\n"
#elif VERBOSE == 2
#  define PRINT_ERROR(x) std::cerr << "[TileDB::ArraySchema] Error: " \
                                   << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB::ArraySchema] Warning: " \
                                     << x << ".\n"
#else
#  define PRINT_ERROR(x) do { } while(0) 
#  define PRINT_WARNING(x) do { } while(0) 
#endif

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArraySchema::ArraySchema() {
  cell_num_per_tile_ = -1;
  domain_ = NULL;
  tile_extents_ = NULL;
}

ArraySchema::~ArraySchema() {
  if(domain_ != NULL)
    free(domain_);

  if(tile_extents_ != NULL)
    free(tile_extents_);
}

/* ****************************** */
/*            ACCESSORS           */
/* ****************************** */

const std::string& ArraySchema::array_name() const {
  return array_name_;
}

const std::string& ArraySchema::attribute(int attribute_id) const {
  assert(attribute_id>= 0 && attribute_id <= attribute_num_);

  return attributes_[attribute_id];
}

int ArraySchema::attribute_id(const std::string& attribute) const {
  for(int i=0; i<attribute_num_; ++i) {
    if(attributes_[i] == attribute)
      return i;
  }

  // Attribute not found
  return TILEDB_AS_ERR;
}

int ArraySchema::attribute_num() const {
  return attribute_num_;
}

const std::vector<std::string>& ArraySchema::attributes() const {
  return attributes_;
}

ArraySchema::Compression ArraySchema::compression(int attribute_id) const {
  assert(attribute_id >= 0 && attribute_id <= attribute_num_);

  return compression_[attribute_id];
}

int64_t ArraySchema::cell_num_per_tile() const {
  return cell_num_per_tile_;
}

size_t ArraySchema::cell_size(int attribute_id) const {
  return cell_sizes_[attribute_id];
}

size_t ArraySchema::coords_size() const {
  return cell_sizes_[attribute_num_];
}

bool ArraySchema::dense() const {
  return dense_;
}

int ArraySchema::get_attribute_ids(
    const std::vector<std::string>& attributes,
    std::vector<int>& attribute_ids) const {
  // Initialization
  attribute_ids.clear();
  int attribute_num = attributes.size();
  int id;

  // Get attribute ids
  for(int i=0; i<attribute_num; ++i) {
    id = attribute_id(attributes[i]);
    if(id == TILEDB_AS_ERR) {
      PRINT_ERROR(std::string("Cannot get attribute id; Attribute '") + 
                  attributes[i] + "' does not exist");
      return TILEDB_AS_ERR;
    } else {
      attribute_ids.push_back(id);
    }
  }

  // Success
  return TILEDB_AS_OK;
}

void ArraySchema::print() const {
  // Array name
  std::cout << "Array name:\n\t" << array_name_ << "\n";
  // Dimension names
  std::cout << "Dimension names:\n";
  for(int i=0; i<dim_num_; ++i)
    std::cout << "\t" << dimensions_[i] << "\n";
  // Attribute names
  std::cout << "Attribute names:\n";
  for(int i=0; i<attribute_num_; ++i)
    std::cout << "\t" << attributes_[i] << "\n";
  // Domain
  std::cout << "Domain:\n";
  if(types_[attribute_num_] == &typeid(int)) {
    int* domain_int = (int*) domain_; 
    for(int i=0; i<dim_num_; ++i) {
      std::cout << "\t"<<  dimensions_[i] << ": [" << domain_int[2*i] << ","
                                          << domain_int[2*i+1] << "]\n";
    }
  } else if(types_[attribute_num_] == &typeid(int64_t)) {
    int64_t* domain_int64 = (int64_t*) domain_; 
    for(int i=0; i<dim_num_; ++i) {
      std::cout << "\t" << dimensions_[i] << ": [" << domain_int64[2*i] << ","
                                          << domain_int64[2*i+1] << "]\n";
    }
  } else if(types_[attribute_num_] == &typeid(float)) {
    float* domain_float = (float*) domain_; 
    for(int i=0; i<dim_num_; ++i) {
      std::cout << "\t" << dimensions_[i] << ": [" << domain_float[2*i] << ","
                                          << domain_float[2*i+1] << "]\n";
    }
  } else if(types_[attribute_num_] == &typeid(double)) {
    double* domain_double = (double*) domain_; 
    for(int i=0; i<dim_num_; ++i) {
      std::cout << "\t" << dimensions_[i] <<  ": [" << domain_double[2*i] << ","
                                          << domain_double[2*i+1] << "]\n";
    }
  }
  // Types
  std::cout << "Types:\n";
  for(int i=0; i<attribute_num_; ++i) {
    if(*types_[i] == typeid(char)) {
      std::cout << "\t" << attributes_[i] << ": char[";
    } else if(*types_[i] == typeid(int)) {
      std::cout << "\t" << attributes_[i] << ": int32[";
    } else if(*types_[i] == typeid(int64_t)) {
      std::cout << "\t" << attributes_[i] << ": int64[";
    } else if(*types_[i] == typeid(float)) {
      std::cout << "\t" << attributes_[i] << ": float32[";
    } else if(*types_[i] == typeid(double)) {
      std::cout << "\t" << attributes_[i] << ": float64[";
    }
    if(val_num_[i] == TILEDB_AS_VAR_SIZE)
      std::cout << "var]\n";
    else
      std::cout << val_num_[i] << "]\n";
  }
  if(key_value_)
    std::cout << "\tCoordinates: char: var\n";
  else if(*types_[attribute_num_] == typeid(int))
    std::cout << "\tCoordinates: int32\n";
  else if(*types_[attribute_num_] == typeid(int64_t))
    std::cout << "\tCoordinates: int64\n";
  else if(*types_[attribute_num_] == typeid(float))
    std::cout << "\tCoordinates: float32\n";
  else if(*types_[attribute_num_] == typeid(double))
    std::cout << "\tCoordinates: float64\n";
  // Cell sizes
  std::cout << "Cell sizes (in bytes):\n";
  for(int i=0; i<=attribute_num_; ++i) {
    std::cout << "\t" << ((i==attribute_num_) ? "Coordinates"  
                                              : attributes_[i]) 
                      << ": ";
    if(cell_sizes_[i] == TILEDB_AS_VAR_SIZE)
      std::cout << "var\n"; 
    else
      std::cout << cell_sizes_[i] << "\n"; 
  }
  // Dense
  std::cout << "Dense:\n\t" << (dense_ ? "true" : "false") << "\n";
  // Key-value
  std::cout << "Key-value:\n\t" << (key_value_ ? "true" : "false") << "\n";
  // Tile types
  std::cout << "Tile types:\n\t" 
            << (tile_extents_ == NULL ? "irregular" : "regular") << "\n";
  // Tile order
  std::cout << "Tile order:\n\t";
  if(tile_extents_ == NULL) {
    std::cout << "-\n";
  } else {
    if(tile_order_ == TILEDB_AS_TO_COLUMN_MAJOR)
      std::cout << "column-major\n";
    else if(tile_order_ == TILEDB_AS_TO_HILBERT)
      std::cout << "hilbert\n";
    else if(tile_order_ == TILEDB_AS_TO_ROW_MAJOR)
      std::cout << "row-major\n";
  }
  // Cell order
  std::cout << "Cell order:\n\t";
  if(cell_order_ == TILEDB_AS_CO_COLUMN_MAJOR)
    std::cout << "column-major\n";
  else if(cell_order_ == TILEDB_AS_CO_HILBERT)
    std::cout << "hilbert\n";
  else if(cell_order_ == TILEDB_AS_CO_ROW_MAJOR)
    std::cout << "row-major\n";
  // Capacity
  std::cout << "Capacity:\n\t";
  if(tile_extents_ != NULL)
    std::cout << "-\n";
  else
    std::cout << capacity_ << "\n";
  // Tile extents
  std::cout << "Tile extents:\n";
  if(tile_extents_ == NULL) {
    std::cout << "-\n"; 
  } else {
    if(types_[attribute_num_] == &typeid(int)) {
      int* tile_extents_int = (int*) tile_extents_;
      for(int i=0; i<dim_num_; ++i)
        std::cout << "\t" << dimensions_[i] << ": " << tile_extents_int[i] << "\n";
    } else if(types_[attribute_num_] == &typeid(int64_t)) {
      int64_t* tile_extents_int64 = (int64_t*) tile_extents_;
      for(int i=0; i<dim_num_; ++i)
        std::cout << "\t" << dimensions_[i] << ": " << tile_extents_int64[i] << "\n";
    } else if(types_[attribute_num_] == &typeid(float)) {
      float* tile_extents_float = (float*) tile_extents_;
      for(int i=0; i<dim_num_; ++i)
        std::cout << "\t" << dimensions_[i] << ": " << tile_extents_float[i] << "\n";
    } else if(types_[attribute_num_] == &typeid(double)) {
      double* tile_extents_double = (double*) tile_extents_;
      for(int i=0; i<dim_num_; ++i)
        std::cout << "\t" << dimensions_[i] << ": " << tile_extents_double[i] << "\n";
    }
  }
  // Consolidation step
  std::cout << "Consolidation step:\n\t" << consolidation_step_ << "\n";
  // Compression types
  std::cout << "Compression types:\n";
  for(int i=0; i<attribute_num_; ++i)
    if(compression_[i] == TILEDB_AS_CMP_GZIP)
      std::cout << "\t" << attributes_[i] << ": GZIP\n";
    else if(compression_[i] == TILEDB_AS_CMP_NONE)
      std::cout << "\t" << attributes_[i] << ": NONE\n";
  if(compression_[attribute_num_] == TILEDB_AS_CMP_GZIP)
    std::cout << "\tCoordinates: GZIP\n";
  else if(compression_[attribute_num_] == TILEDB_AS_CMP_NONE)
    std::cout << "\tCoordinates: NONE\n";
}

// ===== FORMAT =====
// array_name_size(int) 
//     array_name(string)
// dense(bool)
// key_value(bool)
// tile_order(char)
// cell_order(char)
// capacity(int64_t)
// consolidation_step(int)
// attribute_num(int) 
//     attribute_size#1(int) attribute#1(string)
//     attribute_size#2(int) attribute#2(string) 
//     ...
// dim_num(int) 
//    dimension_size#1(int) dimension#1(string)
//    dimension_size#2(int) dimension#2(string)
//    ...
// domain_size(int)
// domain#1_low(double) dim_domain#1_high(double)
// domain#2_low(double) dim_domain#2_high(double)
//  ...
// tile_extents_size(int) 
//     tile_extent#1(double) tile_extent#2(double) ... 
// type#1(char) type#2(char) ... 
// val_num#1(int) val_num#2(int) ... 
// compression#1(char) compression#2(char) ...
int ArraySchema::serialize(
    void*& array_schema_bin,
    size_t& array_schema_bin_size) const {
  // Compute the size of the binary representation of the ArraySchema object
  array_schema_bin_size = compute_bin_size();

  // Allocate memory
  array_schema_bin = malloc(array_schema_bin_size);

  // For easy reference
  char* buffer = (char*) array_schema_bin;
  size_t buffer_size = array_schema_bin_size;
  size_t offset = 0;

  // Copy array_name_
  int array_name_size = array_name_.size();
  assert(offset + sizeof(int) < buffer_size);
  memcpy(buffer + offset, &array_name_size, sizeof(int));
  offset += sizeof(int);
  assert(offset + array_name_size < buffer_size);
  memcpy(buffer + offset, array_name_.c_str(), array_name_size);
  offset += array_name_size;
  // Copy dense_
  assert(offset + sizeof(bool) < buffer_size);
  memcpy(buffer + offset, &dense_, sizeof(bool));
  offset += sizeof(bool);
  // Copy key_value_
  assert(offset + sizeof(bool) < buffer_size);
  memcpy(buffer + offset, &key_value_, sizeof(bool));
  offset += sizeof(bool);
  // Copy tile_order_
  char tile_order = tile_order_;
  assert(offset + sizeof(char) < buffer_size);
  memcpy(buffer + offset, &tile_order, sizeof(char));
  offset += sizeof(char);
  // Copy cell_order_
  char cell_order = cell_order_;
  assert(offset + sizeof(char) < buffer_size);
  memcpy(buffer + offset, &cell_order, sizeof(char));
  offset += sizeof(char);
  // Copy capacity_
  assert(offset + sizeof(int64_t) < buffer_size);
  memcpy(buffer + offset, &capacity_, sizeof(int64_t));
  offset += sizeof(int64_t);
  // Copy consolidation_step_
  assert(offset + sizeof(int) < buffer_size);
  memcpy(buffer + offset, &consolidation_step_, sizeof(int));
  offset += sizeof(int);
  // Copy attributes_
  assert(offset + sizeof(int) < buffer_size);
  memcpy(buffer + offset, &attribute_num_, sizeof(int));
  offset += sizeof(unsigned int);
  int attribute_size;
  for(int i=0; i<attribute_num_; i++) {
    attribute_size = attributes_[i].size();
    assert(offset + sizeof(int) < buffer_size);
    memcpy(buffer + offset, &attribute_size, sizeof(int)); 
    offset += sizeof(int);
    assert(offset + attribute_size < buffer_size);
    memcpy(buffer + offset, attributes_[i].c_str(), attribute_size); 
    offset += attribute_size;
  }
  // Copy dimensions_
  assert(offset + sizeof(int) < buffer_size);
  memcpy(buffer + offset, &dim_num_, sizeof(int));
  offset += sizeof(int);
  int dimension_size;
  for(int i=0; i<dim_num_; i++) {
    dimension_size = dimensions_[i].size();
    assert(offset + sizeof(int) < buffer_size);
    memcpy(buffer + offset, &dimension_size, sizeof(int)); 
    offset += sizeof(int);
    assert(offset + dimension_size < buffer_size);
    memcpy(buffer + offset, dimensions_[i].c_str(), dimension_size); 
    offset += dimension_size;
  }
  // Copy domain_
  int domain_size = 2*coords_size();
  assert(offset + sizeof(int) < buffer_size);
  memcpy(buffer, &domain_size, sizeof(int));
  assert(offset + domain_size < buffer_size);
  memcpy(buffer, domain_, domain_size);
  offset += 2*coords_size();
  // Copy tile_extents_
  int tile_extents_size = ((tile_extents_ == NULL) ? 0 : coords_size()); 
  assert(offset + sizeof(int) < buffer_size);
  memcpy(buffer + offset, &tile_extents_size, sizeof(int));
  offset += sizeof(int);
  if(tile_extents_ != NULL) {
    assert(offset + tile_extents_size < buffer_size);
    memcpy(buffer, tile_extents_, tile_extents_size);
    offset += tile_extents_size;
  }
  // Copy types_
  char type; 
  for(int i=0; i<=attribute_num_; i++) {
    if(*types_[i] == typeid(char))
      type = TILEDB_CHAR;
    else if(*types_[i] == typeid(int))
      type = TILEDB_INT32;
    else if(*types_[i] == typeid(int64_t))
      type = TILEDB_INT64;
    else if(*types_[i] == typeid(float))
      type = TILEDB_FLOAT32;
    else if(*types_[i] == typeid(double))
      type = TILEDB_FLOAT64;
    assert(offset + sizeof(char) < buffer_size);
    memcpy(buffer + offset, &type, sizeof(char));
    offset += sizeof(char);
  }
  // Copy val_num_
  for(int i=0; i<attribute_num_; i++) {
    assert(offset + sizeof(int) < buffer_size);
    memcpy(buffer + offset, &val_num_[i], sizeof(int));
    offset += sizeof(int);
  }
  // Copy compression_
  char compression; 
  for(int i=0; i<=attribute_num_; ++i) {
    compression = compression_[i];
    assert(offset + sizeof(char) <= buffer_size);
    memcpy(buffer + offset, &compression, sizeof(char));
    offset += sizeof(char);
  }
  assert(offset == buffer_size);

  return TILEDB_AS_OK;
}

bool ArraySchema::var_size(int attribute_id) const {
  return cell_sizes_[attribute_id] == TILEDB_AS_VAR_SIZE; 
}

int ArraySchema::var_attribute_num() const {
  return var_attribute_num_;
}

/* ****************************** */
/*             MUTATORS           */
/* ****************************** */

// ===== FORMAT =====
// array_name_size(int) 
//     array_name(string)
// dense(bool)
// key_value(bool)
// tile_order(char)
// cell_order(char)
// capacity(int64_t)
// consolidation_step(int)
// attribute_num(int) 
//     attribute_size#1(int) attribute#1(string)
//     attribute_size#2(int) attribute#2(string) 
//     ...
// dim_num(int) 
//    dimension_size#1(int) dimension#1(string)
//    dimension_size#2(int) dimension#2(string)
//    ...
// domain_size(int)
// domain#1_low(double) dim_domain#1_high(double)
// domain#2_low(double) dim_domain#2_high(double)
//  ...
// tile_extents_size(int) 
//     tile_extent#1(double) tile_extent#2(double) ... 
// type#1(char) type#2(char) ... 
// val_num#1(int) val_num#2(int) ... 
// compression#1(char) compression#2(char) ...
int ArraySchema::deserialize(
    const void* array_schema_bin, 
    size_t array_schema_bin_size) {
  // For easy reference
  const char* buffer = (const char*) array_schema_bin;
  size_t buffer_size = array_schema_bin_size;
  size_t offset = 0;

  // Load array_name_ 
  int array_name_size;
  assert(offset + sizeof(int) < buffer_size);
  memcpy(&array_name_size, buffer + offset, sizeof(int));
  offset += sizeof(int);
  array_name_.resize(array_name_size);
  assert(offset + array_name_size < buffer_size);
  memcpy(&array_name_[0], buffer + offset, array_name_size);
  offset += array_name_size;
  // Load dense_
  assert(offset + sizeof(bool) < buffer_size);
  memcpy(&dense_, buffer + offset, sizeof(bool));
  offset += sizeof(bool);
  // Load key_value_
  assert(offset + sizeof(bool) < buffer_size);
  memcpy(&key_value_, buffer + offset, sizeof(bool));
  offset += sizeof(bool);
  // Load tile_order_ 
  char tile_order;
  assert(offset + sizeof(char) < buffer_size);
  memcpy(&tile_order, buffer + offset, sizeof(char));
  tile_order_ = static_cast<TileOrder>(tile_order);  
  offset += sizeof(char);
  // Load cell_order_
  char cell_order;
  assert(offset + sizeof(char) < buffer_size);
  memcpy(&cell_order, buffer + offset, sizeof(char));
  cell_order_ = static_cast<CellOrder>(cell_order);  
  offset += sizeof(char);
  // Load capacity_
  assert(offset + sizeof(int64_t) < buffer_size);
  memcpy(&capacity_, buffer + offset, sizeof(int64_t));
  offset += sizeof(int64_t);
  // Load consolidation_step_
  assert(offset + sizeof(int) < buffer_size);
  memcpy(&consolidation_step_, buffer + offset, sizeof(int));
  offset += sizeof(int);
  // Load attributes_
  assert(offset + sizeof(int) < buffer_size);
  memcpy(&attribute_num_, buffer + offset, sizeof(int));
  offset += sizeof(int);
  attributes_.resize(attribute_num_);
  int attribute_size;
  for(int i=0; i<attribute_num_; ++i) {
    assert(offset + sizeof(int) < buffer_size);
    memcpy(&attribute_size, buffer+offset, sizeof(int)); 
    offset += sizeof(int);
    attributes_[i].resize(attribute_size);
    assert(offset + attribute_size < buffer_size);
    memcpy(&attributes_[i][0], buffer + offset, attribute_size);
    offset += attribute_size;
  }
  // Load dimensions_
  assert(offset + sizeof(int) < buffer_size);
  memcpy(&dim_num_, buffer + offset, sizeof(int));
  offset += sizeof(int);
  dimensions_.resize(dim_num_);
  int dimension_size;
  for(int i=0; i<dim_num_; ++i) {
    assert(offset + sizeof(int) < buffer_size);
    memcpy(&dimension_size, buffer + offset, sizeof(int)); 
    offset += sizeof(int);
    dimensions_[i].resize(dimension_size);
    assert(offset  + dimension_size < buffer_size);
    memcpy(&dimensions_[i][0], buffer + offset, dimension_size); 
    offset += dimension_size;
  }
  // Load domain_
  int domain_size;
  assert(offset + sizeof(int) < buffer_size);
  memcpy(&domain_size, buffer + offset, sizeof(int));
  offset += sizeof(int);
  assert(offset + domain_size < buffer_size);
  memcpy(domain_, buffer + offset, domain_size); 
  offset += domain_size;
  // Load tile_extents_
  int tile_extents_size;
  assert(offset + sizeof(int) < buffer_size);
  memcpy(&tile_extents_size, buffer + offset, sizeof(int));
  offset += sizeof(int);
  if(tile_extents_size == 0) {
    tile_extents_ = NULL;
  } else {
    assert(offset + tile_extents_size < buffer_size);
    memcpy(tile_extents_, buffer + offset, tile_extents_size);
    offset += tile_extents_size;
  }
  // Load types_ and set type sizes
  char type;
  types_.resize(attribute_num_+1); 
  type_sizes_.resize(attribute_num_+1);
  for(int i=0; i<=attribute_num_; ++i) {
    assert(offset + sizeof(char) < buffer_size);
    memcpy(&type, buffer + offset, sizeof(char));
    offset += sizeof(char);
    if(type == TILEDB_CHAR) {
      types_[i] = &typeid(char);
      type_sizes_[i] = sizeof(char);
    } else if(type == TILEDB_INT32) {
      types_[i] = &typeid(int);
      type_sizes_[i] = sizeof(int);
    } else if(type == TILEDB_INT64) {
      types_[i] = &typeid(int64_t);
      type_sizes_[i] = sizeof(int64_t);
    } else if(type == TILEDB_FLOAT32) {
      types_[i] = &typeid(float);
      type_sizes_[i] = sizeof(float);
    } else if(type == TILEDB_FLOAT64) {
      types_[i] = &typeid(double);
      type_sizes_[i] = sizeof(double);
    }
  }
  // Load val_num_
  var_attribute_num_ = 0;
  val_num_.resize(attribute_num_); 
  for(int i=0; i<attribute_num_; ++i) {
    assert(offset + sizeof(int) < buffer_size);
    memcpy(&val_num_[i], buffer + offset, sizeof(int));
    if(val_num_[i] == TILEDB_AS_VAR_SIZE)
      ++var_attribute_num_;
    offset += sizeof(int);
  }
  // Load compression_
  char compression;
  for(int i=0; i<=attribute_num_; ++i) {
    assert(offset + sizeof(char) <= buffer_size);
    memcpy(&compression, buffer + offset, sizeof(char));
    offset += sizeof(char);
    compression_.push_back(static_cast<Compression>(compression));
  }
  assert(offset == buffer_size);
 
  // Add extra coordinate attribute
  attributes_.push_back(TILEDB_COORDS_NAME);
  // Set cell sizes
  cell_sizes_.resize(attribute_num_+1);
  for(int i=0; i<= attribute_num_; ++i) 
    cell_sizes_[i] = compute_cell_size(i);

  // Compute number of cells per tile
  compute_cell_num_per_tile();

  return TILEDB_AS_OK;
}

int ArraySchema::init(const ArraySchemaC* array_schema_c) {
  // Set array name
  set_array_name(array_schema_c->array_name_);
  // Set attributes
  if(set_attributes(
      array_schema_c->attributes_, 
      array_schema_c->attribute_num_) == TILEDB_AS_ERR)
    return TILEDB_AS_ERR;
  // Set capacity
  set_capacity(array_schema_c->capacity_);
  // Set dimensions
  if(set_dimensions(
      array_schema_c->dimensions_, 
      array_schema_c->dim_num_) == TILEDB_AS_ERR)
    return TILEDB_AS_ERR;
  // Set compression
  if(set_compression(array_schema_c->compression_) == TILEDB_AS_ERR)
    return TILEDB_AS_ERR;
  // Set consolidation step
  set_consolidation_step(array_schema_c->consolidation_step_);
  // Set dense
  set_dense(array_schema_c->dense_);
  // Set cell order
  if(set_cell_order(array_schema_c->cell_order_) == TILEDB_AS_ERR)
    return TILEDB_AS_ERR;
  // Set tile order
  if(set_tile_order(array_schema_c->tile_order_) == TILEDB_AS_ERR)
    return TILEDB_AS_ERR;

  // Set types
  if(set_types(array_schema_c->types_) == TILEDB_AS_ERR)
    return TILEDB_AS_ERR;
  // Set domain
  if(set_domain(array_schema_c->domain_) == TILEDB_AS_ERR)
    return TILEDB_AS_ERR;
  // Set tile extents
  if(set_tile_extents(array_schema_c->tile_extents_) == TILEDB_AS_ERR)
    return TILEDB_AS_ERR;
  // Compute number of cells per tile
  compute_cell_num_per_tile();

  return TILEDB_AS_OK;
}

void ArraySchema::set_array_name(const char* array_name) {
  // Get real array name
  std::string array_name_real = real_dir(array_name);

  // Set array name
  array_name_ = array_name_real;
}

int ArraySchema::set_attributes(
    const char** attributes,
    int attribute_num) {
  // Sanity check on attribute number
  if(attribute_num <= 0) {
    PRINT_ERROR("Cannot set attributes; "
                "The number of attributes must be positive");
    return TILEDB_AS_ERR;
  }

  // Set attributes and attribute number
  for(int i=0; i<attribute_num; ++i) 
    attributes_.push_back(attributes[i]);
  attribute_num_ = attribute_num;

  // Append extra coordinates name
  attributes_.push_back(TILEDB_COORDS_NAME);

  // Check for duplicate attribute names
  if(has_duplicates(attributes_)) {
    PRINT_ERROR("Cannot set attributes; Duplicate attribute names");
    return TILEDB_AS_ERR;
  }

  // Check if a dimension has the same name as an attribute 
  if(intersect(attributes_, dimensions_)) {
    PRINT_ERROR("Cannot set attributes; Attribute name same as dimension name");
    return TILEDB_AS_ERR;
  }

  return TILEDB_AS_OK;
}

void ArraySchema::set_capacity(int capacity) {
  // Set capacity
  if(capacity > 0)
    capacity_ = capacity;
  else
    capacity_ = TILEDB_AS_CAPACITY;
}

int ArraySchema::set_cell_order(const char* cell_order) {
  // Set cell order
  if(cell_order == NULL) {
    cell_order_ = TILEDB_AS_CELL_ORDER;
  } else if(!strcmp(cell_order, "row-major")) {
    cell_order_ = TILEDB_AS_CO_ROW_MAJOR;
  } else if(!strcmp(cell_order, "column-major")) {
    cell_order_ = TILEDB_AS_CO_COLUMN_MAJOR;
  } else if(!strcmp(cell_order, "hilbert")) {
    if(dense_) {
      PRINT_ERROR("Cannot set cell order; Dense arrays do not support "
                  "hilbert order");
      return TILEDB_AS_ERR;
    }
    cell_order_ = TILEDB_AS_CO_HILBERT;
  } else {
    PRINT_ERROR(std::string("Cannot set cell order; Invalid cell order '") + 
                cell_order + "'");
    return TILEDB_AS_ERR;
  }

  return TILEDB_AS_OK;
}

int ArraySchema::set_compression(const char** compression) {
  // Set compression  
  if(compression == NULL) {
    for(int i=0; i<attribute_num_+1; ++i)
      compression_.push_back(TILEDB_AS_CMP_NONE);
  } else {
    for(int i=0; i<attribute_num_+1; ++i) {
      if(!strcmp(compression[i], "NONE")) { 
        compression_.push_back(TILEDB_AS_CMP_NONE);
      } else if(!strcmp(compression[i], "GZIP")) {
        compression_.push_back(TILEDB_AS_CMP_GZIP);
      } else {
        PRINT_ERROR(std::string("Cannot set compression; "
                    "Invalid compression type '") + 
                    compression[i] + "'");
        return TILEDB_AS_ERR;
      }
    }
  }

  return TILEDB_AS_OK;
}

void ArraySchema::set_consolidation_step(int consolidation_step) {
  // Set consilidation step
  if(consolidation_step > 0)
    consolidation_step_ = consolidation_step;
  else
    consolidation_step_ = TILEDB_AS_CONSOLIDATION_STEP;
}

void ArraySchema::set_dense(int dense) {
  dense_ = dense;
}

int ArraySchema::set_dimensions(
    const char** dimensions,
    int dim_num) {
  // Sanity check on dimension number
  if(dim_num <= 0) {
    PRINT_ERROR("Cannot set dimensions; "
                "The number of dimensions must be positive");
    return TILEDB_AS_ERR;
  }

  // Set dimensions and dimension number
  for(int i=0; i<dim_num; ++i) 
    dimensions_.push_back(dimensions[i]);
  dim_num_ = dim_num;

  // Check for duplicate dimension names
  if(has_duplicates(dimensions_)) {
    PRINT_ERROR("Cannot set dimensions; Duplicate dimension names");
    return TILEDB_AS_ERR;
  }

  // Check if a dimension has the same name as an attribute 
  if(intersect(attributes_, dimensions_)) {
    PRINT_ERROR("Cannot set dimensions; Attribute name same as dimension name");
    return TILEDB_AS_ERR;
  }

  return TILEDB_AS_OK;
}

int ArraySchema::set_domain(const void* domain) {
  // Sanity check
  if(domain == NULL) {
    PRINT_ERROR("Cannot set domain; Domain not provided");
    return TILEDB_AS_ERR;
  }

  // Clear domain
  if(domain_ != NULL)
    free(domain_);

  // Set domain
  size_t domain_size = 2*coords_size();
  domain_ = malloc(domain_size);
  memcpy(domain_, domain, domain_size);

  // Sanity check
  if(types_[attribute_num_] == &typeid(int)) {
    int* domain_int = (int*) domain_;
    for(int i=0; i<dim_num_; ++i) {
      if(domain_int[2*i] > domain_int[2*i+1]) {
        PRINT_ERROR("Cannot set domain; Lower domain bound larger than its "
                    "corresponding upper");
        return TILEDB_AS_ERR;
      }
    }
  } else if(types_[attribute_num_] == &typeid(int64_t)) {
    int64_t* domain_int64 = (int64_t*) domain_;
    for(int i=0; i<dim_num_; ++i) {
      if(domain_int64[2*i] > domain_int64[2*i+1]) {
        PRINT_ERROR("Cannot set domain; Lower domain bound larger than its "
                    "corresponding upper");
        return TILEDB_AS_ERR;
      }
    }
  } else if(types_[attribute_num_] == &typeid(float)) {
    float* domain_float = (float*) domain_;
    for(int i=0; i<dim_num_; ++i) {
      if(domain_float[2*i] > domain_float[2*i+1]) {
        PRINT_ERROR("Cannot set domain; Lower domain bound larger than its "
                    "corresponding upper");
        return TILEDB_AS_ERR;
      }
    }
  } else if(types_[attribute_num_] == &typeid(double)) {
    double* domain_double = (double*) domain_;
    for(int i=0; i<dim_num_; ++i) {
      if(domain_double[2*i] > domain_double[2*i+1]) {
        PRINT_ERROR("Cannot set domain; Lower domain bound larger than its "
                    "corresponding upper");
        return TILEDB_AS_ERR;
      }
    }
  }

  return TILEDB_AS_ERR;
}

int ArraySchema::set_tile_extents(const void* tile_extents) {
  // Dense arrays must have tile extents
  if(tile_extents == NULL && dense_) {
    PRINT_ERROR("Cannot set tile extents; Dense arrays must have tile extents");
    return TILEDB_AS_ERR;
  }

  // Key-value stores must not have tile extents
  if(tile_extents != NULL && key_value_) { 
    PRINT_ERROR("Cannot set tile extents; Key-value arrays must not have tile "
                "extents");
    return TILEDB_AS_ERR;
  }

  // Clear tile extents
  if(tile_extents_ != NULL)
    free(tile_extents_);

  // Set tile extents
  size_t tile_extents_size = coords_size();
  tile_extents_ = malloc(tile_extents_size); 
  memcpy(tile_extents_, tile_extents, tile_extents_size);

  return TILEDB_AS_OK;
}

int ArraySchema::set_tile_order(const char* tile_order) {
  // Set tile order
  if(tile_order == NULL) {
    tile_order_ = TILEDB_AS_TILE_ORDER;
  } else if(!strcmp(tile_order, "row-major")) {
    tile_order_ = TILEDB_AS_TO_ROW_MAJOR;
  } else if(!strcmp(tile_order, "column-major")) {
    tile_order_ = TILEDB_AS_TO_COLUMN_MAJOR;
  } else if(!strcmp(tile_order, "hilbert")) {
    if(dense_) {
      PRINT_ERROR("Cannot set tile order; Dense arrays do not support "
                  "hilbert order");
      return TILEDB_AS_ERR;
    }
    tile_order_ = TILEDB_AS_TO_HILBERT;
  } else {
    PRINT_ERROR(std::string("Cannot set tile order; Invalid tile order '") + 
                tile_order + "'");
    return TILEDB_AS_ERR;
  }

  return TILEDB_AS_OK;
}

int ArraySchema::set_types(const char** types) {
  // Sanity check
  if(types == NULL) {
    PRINT_ERROR("Cannot set types; Types not provided");
    return TILEDB_AS_ERR;
  }

  // Set attribute types
  std::string type_val_num;
  std::string type;
  int num;
  var_attribute_num_ = 0;

  for(int i=0; i<attribute_num_; ++i) { 
    type_val_num = types[i];
    char* token = strtok(&type_val_num[0], ":"); // Type extraction
    type = token;    
    token = strtok(NULL, ":"); // Number of attribute values per cell extraction
    if(token == NULL) { // Missing number of attribute values per cell
      val_num_.push_back(1);
    } else {
      // Process next token
      if(!strcmp(token, "var")) { // Variable-sized cell
        val_num_.push_back(TILEDB_AS_VAR_SIZE);
        ++var_attribute_num_;
      } else if(!is_positive_integer(token)) { 
        PRINT_ERROR("Cannot set types; The number of attribute values per "
                    "cell must be a positive integer");
        return TILEDB_AS_ERR;
      } else { // Valid number of attribute values per cell
        sscanf(token, "%d", &num);
        val_num_.push_back(num);
      }

      // No other token should follow
      token = strtok(NULL, ":");
      if(token != NULL) {
        PRINT_ERROR("Cannot set types; Redundant arguments");
        return TILEDB_AS_ERR;
      }
    }

    if(type == "char") {
      types_.push_back(&typeid(char));
    } else if(type == "int32") {
      types_.push_back(&typeid(int));
    } else if(type == "int64") {
      types_.push_back(&typeid(int64_t));
    } else if(type == "float32") {
      types_.push_back(&typeid(float));
    } else if(type == "float64") {
      types_.push_back(&typeid(double));
    } else {
      PRINT_ERROR(std::string("Cannot set types; Invalid attribute type '") + 
                  type + "'");
      return TILEDB_AS_ERR;
    }
  } 

  // Set coordinate type
  type = types[attribute_num_];
  if(type == "char:var") { // Key value - create 4 dimensions for the hashes
    types_.push_back(&typeid(int));
    dim_num_ = 4;
    key_value_ = true;
    std::string dimension = dimensions_.front();
    dimensions_.resize(dim_num_);
    dimensions_[0] = dimension + "_1";
    dimensions_[1] = dimension + "_2";
    dimensions_[2] = dimension + "_3";
    dimensions_[3] = dimension + "_4";
  } else {
    key_value_ = false;

    if(dense_ && (type == "float32" || type == "float64")) {
      PRINT_ERROR("Cannot set types; Dense arrays may only "
                  "have coordinates of type \"int32\" or \"int64\"");
      return TILEDB_AS_ERR;
    }

    if(type == "int32") {
      types_.push_back(&typeid(int));
    } else if(type == "int64") {
      types_.push_back(&typeid(int64_t));
    } else if(type == "float32") {
      types_.push_back(&typeid(float));
    } else if(type == "float64") {
      types_.push_back(&typeid(double));
    } else {
      PRINT_ERROR(std::string("Invalid coordinates type '") + type + "'");
      return TILEDB_AS_ERR;
    }
  }

  // Set type sizes
  type_sizes_.resize(attribute_num_ + 1);
  for(int i=0; i < attribute_num_+1; ++i)
    type_sizes_[i] = compute_type_size(i);

  // Set cell sizes
  cell_sizes_.resize(attribute_num_+1);
  for(int i=0; i < attribute_num_+1; ++i) 
    cell_sizes_[i] = compute_cell_size(i);

  return TILEDB_AS_OK;
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

// ===== FORMAT =====
// array_name_size(int)
//     array_name(string)
// dense(bool)
// key_value(bool)
// tile_order(char)
// cell_order(char)
// capacity(int64_t)
// consolidation_step(int)
// attribute_num(int) 
//     attribute_size#1(int) attribute#1(string)
//     attribute_size#2(int) attribute#2(string) 
//     ...
// dim_num(int) 
//    dimension_size#1(int) dimension#1(string)
//    dimension_size#2(int) dimension#2(string)
//    ...
// domain_size(int)
// domain#1_low(coordinates type) dim_domain#1_high(coordinates type)
// domain#2_low(coordinates type) dim_domain#2_high(coordinates type)
//  ...
// tile_extents_size(int) 
//     tile_extent#1(coordinates type) tile_extent#2(coordinates type) ... 
// type#1(char) type#2(char) ... 
// val_num#1(int) val_num#2(int) ... 
// compression#1(char) compression#2(char) ...
size_t ArraySchema::compute_bin_size() const {
  // Initialization
  size_t bin_size = 0;

  // Size for array_name_ 
  bin_size += sizeof(int) + array_name_.size();
  // Size for dense_
  bin_size += sizeof(bool);
  // Size for key_value_
  bin_size += sizeof(bool);
  // Size for tile_order_ and cell_order_
  bin_size += 2 * sizeof(char);
  // Size for capacity_ 
  bin_size += sizeof(int64_t);
  // Size for consolidation_step__ 
  bin_size += sizeof(int);
  // Size for attribute_num_ and attributes_ 
  bin_size += sizeof(int);
  for(int i=0; i<attribute_num_; ++i)
    bin_size += sizeof(int) + attributes_[i].size();
  // Size for dim_num and dimensions_
  bin_size += sizeof(int);
  for(int i=0; i<dim_num_; ++i)
    bin_size += sizeof(int) + dimensions_[i].size();
  // Size for domain_
  bin_size += sizeof(int) + 2*coords_size();
  // Size for tile_extents_ 
  bin_size += sizeof(int) + ((tile_extents_ == NULL) ? 0 : coords_size()); 
  // Size for types_
  bin_size += (attribute_num_+1) * sizeof(char);
  // Size for val_num_
  bin_size += attribute_num_ * sizeof(int);
  // Size for compression_
  bin_size += (attribute_num_+1) * sizeof(char);

  return bin_size;
}

void ArraySchema::compute_cell_num_per_tile() {
  if(tile_extents_ == NULL)
    return;

  // Compute for the first time
  if(types_[attribute_num_] == &typeid(int)) {
    int* tile_extents = (int*) tile_extents_;
    cell_num_per_tile_ = 1;
    for(int i=0; i<dim_num_; ++i)
      cell_num_per_tile_ *= tile_extents[i]; 
  } else if(types_[attribute_num_] == &typeid(int64_t)) {
    int64_t* tile_extents = (int64_t*) tile_extents_;
    cell_num_per_tile_ = 1;
    for(int i=0; i<dim_num_; ++i)
      cell_num_per_tile_ *= tile_extents[i]; 
  }
}

size_t ArraySchema::compute_cell_size(int i) const {
  assert(i>= 0 && i <= attribute_num_);

  // Variable-sized cell
  if(i<attribute_num_ && val_num_[i] == TILEDB_AS_VAR_SIZE)
    return TILEDB_AS_VAR_SIZE;

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
