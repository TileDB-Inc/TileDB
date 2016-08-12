/*
 * @file   array_schema.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
#include "constants.h"
#include "utils.h"
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <iostream>




/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifdef VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_AS_ERRMSG << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif




/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

std::string tiledb_as_errmsg = "";




/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArraySchema::ArraySchema() {
  cell_num_per_tile_ = -1;
  coords_for_hilbert_ = NULL;
  domain_ = NULL;
  hilbert_curve_ = NULL;
  tile_extents_ = NULL;
  tile_domain_ = NULL;
  tile_coords_aux_ = NULL;
}

ArraySchema::~ArraySchema() {
  if(coords_for_hilbert_ != NULL)
    delete [] coords_for_hilbert_;

  if(domain_ != NULL)
    free(domain_);

  if(hilbert_curve_ != NULL)
    delete hilbert_curve_;

  if(tile_extents_ != NULL)
    free(tile_extents_);

  if(tile_domain_ != NULL)
    free(tile_domain_);

  if(tile_coords_aux_ != NULL)
    free(tile_coords_aux_);
}




/* ****************************** */
/*            ACCESSORS           */
/* ****************************** */

const std::string& ArraySchema::array_name() const {
  return array_name_;
}


void ArraySchema::array_schema_export(ArraySchemaC* array_schema_c) const {
  // Set array name
  size_t array_name_len = array_name_.size(); 
  array_schema_c->array_name_ = (char*) malloc(array_name_len+1);
  strcpy(array_schema_c->array_name_, array_name_.c_str());

  // Set attributes and number of attributes.
  array_schema_c->attribute_num_ = attribute_num_;
  array_schema_c->attributes_ = 
      (char**) malloc(attribute_num_*sizeof(char*));
  for(int i=0; i<attribute_num_; ++i) { 
    size_t attribute_len = attributes_[i].size();
    array_schema_c->attributes_[i] = (char*) malloc(attribute_len+1);
    strcpy(array_schema_c->attributes_[i], attributes_[i].c_str());
  }

  // Set dimensions
  array_schema_c->dim_num_ = dim_num_; 
  array_schema_c->dimensions_ = (char**) malloc(dim_num_*sizeof(char*));
  for(int i=0; i<dim_num_; ++i) { 
    size_t dimension_len = dimensions_[i].size();
    array_schema_c->dimensions_[i] = (char*) malloc(dimension_len+1);
    strcpy(array_schema_c->dimensions_[i], dimensions_[i].c_str());
  }

  // Set dense
  array_schema_c->dense_ = dense_;

  // Set domain
  size_t coords_size = this->coords_size();
  array_schema_c->domain_ = malloc(2*coords_size); 
  memcpy(array_schema_c->domain_, domain_, 2*coords_size);

  // Set tile extents
  if(tile_extents_ == NULL) {
    array_schema_c->tile_extents_ = NULL;
  } else {
    array_schema_c->tile_extents_ = malloc(coords_size); 
    memcpy(array_schema_c->tile_extents_, tile_extents_, coords_size);
  }

  // Set types
  array_schema_c->types_ = (int*) malloc((attribute_num_+1)*sizeof(int));
  for(int i=0; i<attribute_num_+1; ++i)
    array_schema_c->types_[i] = types_[i];

  // Set cell val num
  array_schema_c->cell_val_num_ = 
      (int*) malloc((attribute_num_)*sizeof(int));
  for(int i=0; i<attribute_num_; ++i)
    array_schema_c->cell_val_num_[i] = cell_val_num_[i];

  // Set cell order
  array_schema_c->cell_order_ = cell_order_;

  // Set tile order
  array_schema_c->tile_order_ = tile_order_;

  // Set capacity
  array_schema_c->capacity_ = capacity_;

  // Set compression
  array_schema_c->compression_ = 
      (int*) malloc((attribute_num_+1)*sizeof(int));
  for(int i=0; i<attribute_num_+1; ++i)
    array_schema_c->compression_[i] = compression_[i];
}

void ArraySchema::array_schema_export(
    MetadataSchemaC* metadata_schema_c) const {
  // Set metadata name
  size_t array_name_len = array_name_.size(); 
  metadata_schema_c->metadata_name_ = (char*) malloc(array_name_len+1);
  strcpy(metadata_schema_c->metadata_name_, array_name_.c_str());

  // Set attributes and number of attributes
  metadata_schema_c->attribute_num_ = attribute_num_ - 1;
  metadata_schema_c->attributes_ = 
      (char**) malloc((attribute_num_-1)*sizeof(char*));
  for(int i=0; i<attribute_num_-1; ++i) { 
    size_t attribute_len = attributes_[i].size();
    metadata_schema_c->attributes_[i] = (char*) malloc(attribute_len+1);
    strcpy(metadata_schema_c->attributes_[i], attributes_[i].c_str());
  }

  // Set types
  metadata_schema_c->types_ = (int*) malloc((attribute_num_-1)*sizeof(int));
  for(int i=0; i<attribute_num_-1; ++i)
    metadata_schema_c->types_[i] = types_[i];

  // Set cell val num
  metadata_schema_c->cell_val_num_ = 
      (int*) malloc((attribute_num_-1)*sizeof(int));
  for(int i=0; i<attribute_num_-1; ++i)
    metadata_schema_c->cell_val_num_[i] = cell_val_num_[i];

  // Set capacity
  metadata_schema_c->capacity_ = capacity_;

  // Set compression
  metadata_schema_c->compression_ = 
      (int*) malloc(attribute_num_*sizeof(int));
  for(int i=0; i<attribute_num_; ++i)
    metadata_schema_c->compression_[i] = compression_[i];
}

const std::string& ArraySchema::attribute(int attribute_id) const {
  assert(attribute_id >= 0 && attribute_id <= attribute_num_+1);

  // Special case for the search attribute (same as coordinates)
  if(attribute_id == attribute_num_+1)
    attribute_id = attribute_num_;

  return attributes_[attribute_id];
}

int ArraySchema::attribute_id(const std::string& attribute) const {
  // Special case - coordinates
  if(attribute == TILEDB_COORDS)
    return attribute_num_;

  for(int i=0; i<attribute_num_; ++i) {
    if(attributes_[i] == attribute)
      return i;
  }

  // Attribute not found
  std::string errmsg = "Attribute not found";
  PRINT_ERROR(errmsg);
  tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
  return TILEDB_AS_ERR;
}

int ArraySchema::attribute_num() const {
  return attribute_num_;
}

const std::vector<std::string>& ArraySchema::attributes() const {
  return attributes_;
}

int64_t ArraySchema::capacity() const {
  return capacity_;
}

int64_t ArraySchema::cell_num_per_tile() const {
  // Sanity check
  assert(dense_);

  return cell_num_per_tile_;
}

int ArraySchema::cell_order() const {
  return cell_order_;
}

size_t ArraySchema::cell_size(int attribute_id) const {
  // Special case for the search tile
  if(attribute_id == attribute_num_+1)
    attribute_id = attribute_num_;

  return cell_sizes_[attribute_id];
}

int ArraySchema::compression(int attribute_id) const {
  assert(attribute_id >= 0 && attribute_id <= attribute_num_+1);

  // Special case for the "search tile", which is essentially the 
  // coordinates tile
  if(attribute_id == attribute_num_+1)
    attribute_id = attribute_num_;

  return compression_[attribute_id];
}

size_t ArraySchema::coords_size() const {
  return coords_size_;
}

int ArraySchema::coords_type() const {
  return types_[attribute_num_];
}

bool ArraySchema::dense() const {
  return dense_;
}

int ArraySchema::dim_num() const {
  return dim_num_;
}

const void* ArraySchema::domain() const {
  return domain_;
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
      std::string errmsg = 
          std::string("Cannot get attribute id; Attribute '") + 
          attributes[i] + "' does not exist";
      PRINT_ERROR(errmsg);
      tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
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
  if(types_[attribute_num_] == TILEDB_INT32) {
    int* domain_int = (int*) domain_; 
    for(int i=0; i<dim_num_; ++i) {
      std::cout << "\t"<<  dimensions_[i] << ": [" << domain_int[2*i] << ","
                                          << domain_int[2*i+1] << "]\n";
    }
  } else if(types_[attribute_num_] == TILEDB_INT64) {
    int64_t* domain_int64 = (int64_t*) domain_; 
    for(int i=0; i<dim_num_; ++i) {
      std::cout << "\t" << dimensions_[i] << ": [" << domain_int64[2*i] << ","
                                          << domain_int64[2*i+1] << "]\n";
    }
  } else if(types_[attribute_num_] == TILEDB_FLOAT32) {
    float* domain_float = (float*) domain_; 
    for(int i=0; i<dim_num_; ++i) {
      std::cout << "\t" << dimensions_[i] << ": [" << domain_float[2*i] << ","
                                          << domain_float[2*i+1] << "]\n";
    }
  } else if(types_[attribute_num_] == TILEDB_FLOAT64) {
    double* domain_double = (double*) domain_; 
    for(int i=0; i<dim_num_; ++i) {
      std::cout << "\t" << dimensions_[i] <<  ": [" << domain_double[2*i] << ","
                                          << domain_double[2*i+1] << "]\n";
    }
  }
  // Types
  std::cout << "Types:\n";
  for(int i=0; i<attribute_num_; ++i) {
    if(types_[i] == TILEDB_CHAR) {
      std::cout << "\t" << attributes_[i] << ": char[";
    } else if(types_[i] == TILEDB_INT32) {
      std::cout << "\t" << attributes_[i] << ": int32[";
    } else if(types_[i] == TILEDB_INT64) {
      std::cout << "\t" << attributes_[i] << ": int64[";
    } else if(types_[i] == TILEDB_FLOAT32) {
      std::cout << "\t" << attributes_[i] << ": float32[";
    } else if(types_[i] == TILEDB_FLOAT64) {
      std::cout << "\t" << attributes_[i] << ": float64[";
    }
    if(cell_val_num_[i] == TILEDB_VAR_NUM)
      std::cout << "var]\n";
    else
      std::cout << cell_val_num_[i] << "]\n";
  }
  if(types_[attribute_num_] == TILEDB_INT32)
    std::cout << "\tCoordinates: int32\n";
  else if(types_[attribute_num_] == TILEDB_INT64)
    std::cout << "\tCoordinates: int64\n";
  else if(types_[attribute_num_] == TILEDB_FLOAT32)
    std::cout << "\tCoordinates: float32\n";
  else if(types_[attribute_num_] == TILEDB_FLOAT64)
    std::cout << "\tCoordinates: float64\n";
  // Cell sizes
  std::cout << "Cell sizes (in bytes):\n";
  for(int i=0; i<=attribute_num_; ++i) {
    std::cout << "\t" << ((i==attribute_num_) ? "Coordinates"  
                                              : attributes_[i]) 
                      << ": ";
    if(cell_sizes_[i] == TILEDB_VAR_SIZE)
      std::cout << "var\n"; 
    else
      std::cout << cell_sizes_[i] << "\n"; 
  }
  // Dense
  std::cout << "Dense:\n\t" << (dense_ ? "true" : "false") << "\n";
  // Tile type
  std::cout << "Tile types:\n\t" 
            << (tile_extents_ == NULL ? "irregular" : "regular") << "\n";
  // Tile order
  std::cout << "Tile order:\n\t";
  if(tile_extents_ == NULL) {
    std::cout << "-\n";
  } else {
    if(tile_order_ == TILEDB_COL_MAJOR)
      std::cout << "column-major\n";
    else if(tile_order_ == TILEDB_HILBERT)
      std::cout << "hilbert\n";
    else if(tile_order_ == TILEDB_ROW_MAJOR)
      std::cout << "row-major\n";
  }
  // Cell order
  std::cout << "Cell order:\n\t";
  if(cell_order_ == TILEDB_COL_MAJOR)
    std::cout << "column-major\n";
  else if(cell_order_ == TILEDB_HILBERT)
    std::cout << "hilbert\n";
  else if(cell_order_ == TILEDB_ROW_MAJOR)
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
    if(types_[attribute_num_] == TILEDB_INT32) {
      int* tile_extents_int = (int*) tile_extents_;
      for(int i=0; i<dim_num_; ++i)
        std::cout << "\t" << dimensions_[i] << ": " 
                  << tile_extents_int[i] << "\n";
    } else if(types_[attribute_num_] == TILEDB_INT64) {
      int64_t* tile_extents_int64 = (int64_t*) tile_extents_;
      for(int i=0; i<dim_num_; ++i)
        std::cout << "\t" << dimensions_[i] << ": " 
                  << tile_extents_int64[i] << "\n";
    } else if(types_[attribute_num_] == TILEDB_FLOAT32) {
      float* tile_extents_float = (float*) tile_extents_;
      for(int i=0; i<dim_num_; ++i)
        std::cout << "\t" << dimensions_[i] << ": " 
                  << tile_extents_float[i] << "\n";
    } else if(types_[attribute_num_] == TILEDB_FLOAT64) {
      double* tile_extents_double = (double*) tile_extents_;
      for(int i=0; i<dim_num_; ++i)
        std::cout << "\t" << dimensions_[i] << ": " 
                  << tile_extents_double[i] << "\n";
    }
  }
  // Compression type
  std::cout << "Compression type:\n";
  for(int i=0; i<attribute_num_; ++i)
    if(compression_[i] == TILEDB_GZIP)
      std::cout << "\t" << attributes_[i] << ": GZIP\n";
    else if(compression_[i] == TILEDB_NO_COMPRESSION)
      std::cout << "\t" << attributes_[i] << ": NONE\n";
  if(compression_[attribute_num_] == TILEDB_GZIP)
    std::cout << "\tCoordinates: GZIP\n";
  else if(compression_[attribute_num_] == TILEDB_NO_COMPRESSION)
    std::cout << "\tCoordinates: NONE\n";
}

// ===== FORMAT =====
// array_name_size(int) 
//     array_name(string)
// dense(bool)
// tile_order(char)
// cell_order(char)
// capacity(int64_t)
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
// cell_val_num#1(int) cell_val_num#2(int) ... 
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
  memcpy(buffer + offset, &array_name_[0], array_name_size);
  offset += array_name_size;
  // Copy dense_
  assert(offset + sizeof(bool) < buffer_size);
  memcpy(buffer + offset, &dense_, sizeof(bool));
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
  // Copy attributes_
  assert(offset + sizeof(int) < buffer_size);
  memcpy(buffer + offset, &attribute_num_, sizeof(int));
  offset += sizeof(int);
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
  memcpy(buffer + offset, &domain_size, sizeof(int));
  offset += sizeof(int);
  assert(offset + domain_size < buffer_size);
  memcpy(buffer + offset, domain_, domain_size);
  offset += 2*coords_size();
  // Copy tile_extents_
  int tile_extents_size = ((tile_extents_ == NULL) ? 0 : coords_size()); 
  assert(offset + sizeof(int) < buffer_size);
  memcpy(buffer + offset, &tile_extents_size, sizeof(int));
  offset += sizeof(int);
  if(tile_extents_ != NULL) {
    assert(offset + tile_extents_size < buffer_size);
    memcpy(buffer + offset, tile_extents_, tile_extents_size);
    offset += tile_extents_size;
  }
  // Copy types_
  char type; 
  for(int i=0; i<=attribute_num_; i++) {
    type = static_cast<char>(types_[i]);
    assert(offset + sizeof(char) < buffer_size);
    memcpy(buffer + offset, &type, sizeof(char));
    offset += sizeof(char);
  }
  // Copy cell_val_num_
  for(int i=0; i<attribute_num_; i++) {
    assert(offset + sizeof(int) < buffer_size);
    memcpy(buffer + offset, &cell_val_num_[i], sizeof(int));
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

  // Success
  return TILEDB_AS_OK;
}

template<class T>
int ArraySchema::subarray_overlap(
    const T* subarray_a, 
    const T* subarray_b, 
    T* overlap_subarray) const {
  // Get overlap range
  for(int i=0; i<dim_num_; ++i) {
    overlap_subarray[2*i] = 
        std::max(subarray_a[2*i], subarray_b[2*i]);
    overlap_subarray[2*i+1] = 
        std::min(subarray_a[2*i+1], subarray_b[2*i+1]);
  }

  // Check overlap
  int overlap = 1;
  for(int i=0; i<dim_num_; ++i) {
    if(overlap_subarray[2*i] > subarray_b[2*i+1] ||
       overlap_subarray[2*i+1] < subarray_b[2*i]) {
      overlap = 0;
      break;
    }
  }

  // Check partial overlap
  if(overlap == 1) {
    for(int i=0; i<dim_num_; ++i) {
      if(overlap_subarray[2*i] != subarray_b[2*i] ||
         overlap_subarray[2*i+1] != subarray_b[2*i+1]) {
        overlap = 2;
        break;
      }
    }
  }

  // Check contig overlap (not applicable to Hilbert order)
  if(overlap == 2 && cell_order_ != TILEDB_HILBERT) {
    overlap = 3;
    if(cell_order_ == TILEDB_ROW_MAJOR) {           // Row major
      for(int i=1; i<dim_num_; ++i) {
        if(overlap_subarray[2*i] != subarray_b[2*i] ||
           overlap_subarray[2*i+1] != subarray_b[2*i+1]) {
          overlap = 2;
          break;
        }
      }
    } else if(cell_order_ == TILEDB_COL_MAJOR) {    // Column major
      for(int i=dim_num_-2; i>=0; --i) {
        if(overlap_subarray[2*i] != subarray_b[2*i] ||
           overlap_subarray[2*i+1] != subarray_b[2*i+1]) {
          overlap = 2;
          break;
        }
      }
    }
  } 

  // Return
  return overlap;
}


const void* ArraySchema::tile_domain() const {
  return tile_domain_;
}

const void* ArraySchema::tile_extents() const {
  return tile_extents_;
}

int64_t ArraySchema::tile_num() const {
  // Invoke the proper template function 
  if(types_[attribute_num_] == TILEDB_INT32)
    return tile_num<int>();
  else if(types_[attribute_num_] == TILEDB_INT64)
    return tile_num<int64_t>();

  assert(0);
  std::string errmsg = 
      "Unsupported dimensions type for retrieving the "
      "number of tiles";
  PRINT_ERROR(errmsg);
  tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
  return TILEDB_AS_ERR;
}

template<class T>
int64_t ArraySchema::tile_num() const {
  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  int64_t ret = 1;
  for(int i=0; i<dim_num_; ++i) 
    ret *= (domain[2*i+1] - domain[2*i] + 1) / tile_extents[i];

  return ret; 
}

int64_t ArraySchema::tile_num(const void* domain) const {
  // Invoke the proper template function 
  if(types_[attribute_num_] == TILEDB_INT32)
    return tile_num<int>(static_cast<const int*>(domain));
  else if(types_[attribute_num_] == TILEDB_INT64)
    return tile_num<int64_t>(static_cast<const int64_t*>(domain));


  assert(0);
  std::string errmsg = 
      "Unsupported dimensions type for retrieving the "
      "number of tiles";
  PRINT_ERROR(errmsg);
  tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
  return TILEDB_AS_ERR;
}

template<class T>
int64_t ArraySchema::tile_num(const T* domain) const {
  // For easy reference
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  int64_t ret = 1;
  for(int i=0; i<dim_num_; ++i) 
    ret *= (domain[2*i+1] - domain[2*i] + 1) / tile_extents[i];

  return ret; 
}

int ArraySchema::type(int i) const {
  if(i<0 || i>attribute_num_) {
    std::string errmsg = "Cannot retrieve type; Invalid attribute id";
    PRINT_ERROR(errmsg);
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  } else {
    return types_[i];
  }
}

int ArraySchema::var_attribute_num() const {
  int var_attribute_num = 0;
  for(int i=0; i<attribute_num_; ++i)
    if(var_size(i))
      ++var_attribute_num;

  return var_attribute_num;
}

bool ArraySchema::var_size(int attribute_id) const {
  return cell_sizes_[attribute_id] == TILEDB_VAR_SIZE; 
}




/* ****************************** */
/*             MUTATORS           */
/* ****************************** */

// ===== FORMAT =====
// array_name_size(int) 
//     array_name(string)
// dense(bool)
// tile_order(char)
// cell_order(char)
// capacity(int64_t)
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
// cell_val_num#1(int) cell_val_num#2(int) ... 
// compression#1(char) compression#2(char) ...
int ArraySchema::deserialize(
    const void* array_schema_bin, 
    size_t array_schema_bin_size) {
  // For easy reference
  const char* buffer = static_cast<const char*>(array_schema_bin);
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
  // Load tile_order_ 
  char tile_order;
  assert(offset + sizeof(char) < buffer_size);
  memcpy(&tile_order, buffer + offset, sizeof(char));
  tile_order_ = static_cast<int>(tile_order);  
  offset += sizeof(char);
  // Load cell_order_
  char cell_order;
  assert(offset + sizeof(char) < buffer_size);
  memcpy(&cell_order, buffer + offset, sizeof(char));
  cell_order_ = static_cast<int>(cell_order);  
  offset += sizeof(char);
  // Load capacity_
  assert(offset + sizeof(int64_t) < buffer_size);
  memcpy(&capacity_, buffer + offset, sizeof(int64_t));
  offset += sizeof(int64_t);
  // Load attributes_
  assert(offset + sizeof(int) < buffer_size);
  memcpy(&attribute_num_, buffer + offset, sizeof(int));
  offset += sizeof(int);
  attributes_.resize(attribute_num_);
  int attribute_size;
  for(int i=0; i<attribute_num_; ++i) {
    assert(offset + sizeof(int) < buffer_size);
    memcpy(&attribute_size, buffer + offset, sizeof(int)); 
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
  domain_ = malloc(domain_size);
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
    tile_extents_ = malloc(tile_extents_size); 
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
    types_[i] = static_cast<int>(type);
  }
  // Load cell_val_num_
  cell_val_num_.resize(attribute_num_); 
  for(int i=0; i<attribute_num_; ++i) {
    assert(offset + sizeof(int) < buffer_size);
    memcpy(&cell_val_num_[i], buffer + offset, sizeof(int));
    offset += sizeof(int);
  }
  // Load compression_
  char compression;
  for(int i=0; i<=attribute_num_; ++i) {
    assert(offset + sizeof(char) <= buffer_size);
    memcpy(&compression, buffer + offset, sizeof(char));
    offset += sizeof(char);
    compression_.push_back(static_cast<int>(compression));
  }
  assert(offset == buffer_size); 
  // Add extra coordinate attribute
  attributes_.push_back(TILEDB_COORDS);
  // Set cell sizes
  cell_sizes_.resize(attribute_num_+1);
  for(int i=0; i<= attribute_num_; ++i) 
    cell_sizes_[i] = compute_cell_size(i);
  // Set coordinates size
  coords_size_ = cell_sizes_[attribute_num_];

  // Compute number of cells per tile
  compute_cell_num_per_tile();

  // Compute tile domain
  compute_tile_domain();

  // Compute tile offsets
  compute_tile_offsets();

  // Initialize Hilbert curve
  init_hilbert_curve();

  // Initialize static auxiliary variables
  if(tile_coords_aux_ != NULL)
    free(tile_coords_aux_);
  tile_coords_aux_ = malloc(coords_size_*dim_num_);

  // Success
  return TILEDB_AS_OK;
}

int ArraySchema::init(const ArraySchemaC* array_schema_c) {
  // Set array name
  set_array_name(array_schema_c->array_name_);
  // Set attributes
  if(set_attributes(
      array_schema_c->attributes_, 
      array_schema_c->attribute_num_) != TILEDB_AS_OK)
    return TILEDB_AS_ERR;
  // Set capacity
  set_capacity(array_schema_c->capacity_);
  // Set dimensions
  if(set_dimensions(
      array_schema_c->dimensions_, 
      array_schema_c->dim_num_) != TILEDB_AS_OK)
    return TILEDB_AS_ERR;
  // Set compression
  if(set_compression(array_schema_c->compression_) != TILEDB_AS_OK)
    return TILEDB_AS_ERR;
  // Set dense
  set_dense(array_schema_c->dense_);
  // Set number of values per cell
  set_cell_val_num(array_schema_c->cell_val_num_);
  // Set types
  if(set_types(array_schema_c->types_) != TILEDB_AS_OK)
    return TILEDB_AS_ERR;
  // Set tile extents
  if(set_tile_extents(array_schema_c->tile_extents_) != TILEDB_AS_OK)
    return TILEDB_AS_ERR;
  // Set cell order
  if(set_cell_order(array_schema_c->cell_order_) != TILEDB_AS_OK)
    return TILEDB_AS_ERR;
  // Set tile order
  if(set_tile_order(array_schema_c->tile_order_) != TILEDB_AS_OK)
    return TILEDB_AS_ERR;
  // Set domain
  if(set_domain(array_schema_c->domain_) != TILEDB_AS_OK)
    return TILEDB_AS_ERR;

  // Compute number of cells per tile
  compute_cell_num_per_tile();

  // Compute tile domain
  compute_tile_domain();

  // Compute tile offsets
  compute_tile_offsets();

  // Initialize Hilbert curve
  init_hilbert_curve();

  // Initialize static auxiliary variables
  if(tile_coords_aux_ != NULL)
    free(tile_coords_aux_);
  tile_coords_aux_ = malloc(coords_size_*dim_num_);

  // Success
  return TILEDB_AS_OK;
}

int ArraySchema::init(const MetadataSchemaC* metadata_schema_c) {
  // Create an array schema C struct and populate it
  ArraySchemaC array_schema_c;
  array_schema_c.array_name_ = metadata_schema_c->metadata_name_;
  array_schema_c.capacity_ = metadata_schema_c->capacity_;
  array_schema_c.cell_order_ = TILEDB_ROW_MAJOR;
  array_schema_c.tile_order_ = TILEDB_ROW_MAJOR;
  array_schema_c.tile_extents_ = NULL;
  array_schema_c.dense_ = 0;

  // Set attributes
  char** attributes = 
      (char**) malloc((metadata_schema_c->attribute_num_+1)*sizeof(char*));
  size_t attribute_len;
  for(int i=0; i<metadata_schema_c->attribute_num_; ++i) {
    attribute_len = strlen(metadata_schema_c->attributes_[i]);
    attributes[i] = (char*) malloc(attribute_len+1);
    strcpy(attributes[i], metadata_schema_c->attributes_[i]);
  }
  attribute_len = strlen(TILEDB_KEY);
  attributes[metadata_schema_c->attribute_num_] = 
      (char*) malloc(attribute_len+1);
  strcpy(attributes[metadata_schema_c->attribute_num_],TILEDB_KEY);
  array_schema_c.attributes_ = attributes; 
  array_schema_c.attribute_num_ = metadata_schema_c->attribute_num_ + 1;

  // Set dimensions
  char** dimensions = (char**) malloc(4*sizeof(char*));
  size_t dimension_len;
  dimension_len = strlen(TILEDB_AS_KEY_DIM1_NAME); 
  dimensions[0] = (char*) malloc(dimension_len+1);
  strcpy(dimensions[0], TILEDB_AS_KEY_DIM1_NAME); 
  dimension_len = strlen(TILEDB_AS_KEY_DIM2_NAME); 
  dimensions[1] = (char*) malloc(dimension_len+1);
  strcpy(dimensions[1], TILEDB_AS_KEY_DIM2_NAME); 
  dimension_len = strlen(TILEDB_AS_KEY_DIM3_NAME); 
  dimensions[2] = (char*) malloc(dimension_len+1);
  strcpy(dimensions[2], TILEDB_AS_KEY_DIM3_NAME); 
  array_schema_c.dimensions_ = dimensions;
  dimension_len = strlen(TILEDB_AS_KEY_DIM4_NAME); 
  dimensions[3] = (char*) malloc(dimension_len+1);
  strcpy(dimensions[3], TILEDB_AS_KEY_DIM4_NAME); 
  array_schema_c.dimensions_ = dimensions;
  array_schema_c.dim_num_ = 4;

  // Set domain
  int* domain = (int*) malloc(8*sizeof(int));
  for(int i=0; i<4; ++i) {
    domain[2*i] = INT_MIN;
    domain[2*i+1] = INT_MAX;
  }
  array_schema_c.domain_ = domain;

  // Set types
  int* types = (int*) malloc((metadata_schema_c->attribute_num_+2)*sizeof(int));
  for(int i=0; i<metadata_schema_c->attribute_num_; ++i)
    types[i] = metadata_schema_c->types_[i];
  types[metadata_schema_c->attribute_num_] = TILEDB_CHAR;
  types[metadata_schema_c->attribute_num_+1] = TILEDB_INT32;
  array_schema_c.types_ = types;
 
  // Set cell num val
  int* cell_val_num = 
      (int*) malloc((metadata_schema_c->attribute_num_+1)*sizeof(int));
  if(metadata_schema_c->cell_val_num_ == NULL) {
    for(int i=0; i<metadata_schema_c->attribute_num_; ++i)
      cell_val_num[i] = 1;
  } else {
    for(int i=0; i<metadata_schema_c->attribute_num_; ++i)
      cell_val_num[i] = metadata_schema_c->cell_val_num_[i];
  }
  cell_val_num[metadata_schema_c->attribute_num_] = TILEDB_VAR_NUM;
  array_schema_c.cell_val_num_ = cell_val_num;

  // Set compression
  int* compression = 
      (int*) malloc((metadata_schema_c->attribute_num_+2)*sizeof(int));
  if(metadata_schema_c->compression_ == NULL) {
    for(int i=0; i<metadata_schema_c->attribute_num_+1; ++i)
      compression[i] = TILEDB_NO_COMPRESSION;
  } else {
    for(int i=0; i<metadata_schema_c->attribute_num_+1; ++i)
      compression[i] = metadata_schema_c->compression_[i];
  }
  compression[metadata_schema_c->attribute_num_+1] = TILEDB_NO_COMPRESSION;
  array_schema_c.compression_ = compression;

  // Initialize schema through the array schema C struct
  init(&array_schema_c);

  // Clean up
  for(int i=0; i<array_schema_c.attribute_num_; ++i)
    free(attributes[i]);
  free(attributes);
  for(int i=0; i<4; ++i)
    free(dimensions[i]);
  free(dimensions);
  free(domain);
  free(types);
  free(compression);
  free(cell_val_num);

  // Success
  return TILEDB_AS_OK;
}

void ArraySchema::set_array_name(const char* array_name) {
  // Get real array name
  std::string array_name_real = real_dir(array_name);

  // Set array name
  array_name_ = array_name_real;
}

int ArraySchema::set_attributes(
    char** attributes,
    int attribute_num) {
  // Sanity check on attributes
  if(attributes == NULL) {
    std::string errmsg = "Cannot set attributes; No attributes given";
    PRINT_ERROR(errmsg);
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  }

  // Sanity check on attribute number
  if(attribute_num <= 0) {
    std::string errmsg = 
        "Cannot set attributes; "
        "The number of attributes must be positive";
    PRINT_ERROR(errmsg);
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  }

  // Set attributes and attribute number
  for(int i=0; i<attribute_num; ++i) 
    attributes_.push_back(attributes[i]);
  attribute_num_ = attribute_num;

  // Append extra coordinates name
  attributes_.push_back(TILEDB_COORDS);

  // Check for duplicate attribute names
  if(has_duplicates(attributes_)) {
    std::string errmsg = "Cannot set attributes; Duplicate attribute names";
    PRINT_ERROR(errmsg);
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  }

  // Check if a dimension has the same name as an attribute 
  if(intersect(attributes_, dimensions_)) {
    std::string errmsg =
        "Cannot set attributes; Attribute name same as dimension name";
    PRINT_ERROR(errmsg);
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  }

  // Success
  return TILEDB_AS_OK;
}

void ArraySchema::set_capacity(int64_t capacity) {
  assert(capacity >= 0);

  // Set capacity
  if(capacity > 0) 
    capacity_ = capacity;
  else
    capacity_ = TILEDB_AS_CAPACITY;
}

void ArraySchema::set_cell_val_num(const int* cell_val_num) {
  if(cell_val_num == NULL) {
    for(int i=0; i<attribute_num_; ++i)
      cell_val_num_.push_back(1);
  } else {
    for(int i=0; i<attribute_num_; ++i) 
      cell_val_num_.push_back(cell_val_num[i]);
  }
}

int ArraySchema::set_cell_order(int cell_order) {
  // Set cell order
  if(cell_order != TILEDB_ROW_MAJOR &&
     cell_order != TILEDB_COL_MAJOR &&
     cell_order != TILEDB_HILBERT) {
    std::string errmsg = "Cannot set cell order; Invalid cell order";
    PRINT_ERROR(errmsg); 
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  }
  cell_order_ = cell_order;

  // Success
  return TILEDB_AS_OK;
}

int ArraySchema::set_compression(int* compression) {
  // Set compression  
  if(compression == NULL) {
    for(int i=0; i<attribute_num_+1; ++i)
      compression_.push_back(TILEDB_NO_COMPRESSION);
  } else {
    for(int i=0; i<attribute_num_+1; ++i) {
      if(compression[i] != TILEDB_NO_COMPRESSION &&
         compression[i] != TILEDB_GZIP) { 
        std::string errmsg = "Cannot set compression; Invalid compression type";
        PRINT_ERROR(errmsg);
        tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
        return TILEDB_AS_ERR;
      }
      compression_.push_back(compression[i]);
    }
  }

  // Success
  return TILEDB_AS_OK;
}

void ArraySchema::set_dense(int dense) {
  dense_ = dense;
}

int ArraySchema::set_dimensions(
    char** dimensions,
    int dim_num) {
  // Sanity check on dimensions
  if(dimensions == NULL) {
    std::string errmsg = "Cannot set dimensions; No dimensions given";
    PRINT_ERROR(errmsg);
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  }

  // Sanity check on dimension number
  if(dim_num <= 0) {
    std::string errmsg = 
        "Cannot set dimensions; "
        "The number of dimensions must be positive";
    PRINT_ERROR(errmsg);
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  }

  // Set dimensions and dimension number
  for(int i=0; i<dim_num; ++i) 
    dimensions_.push_back(dimensions[i]);
  dim_num_ = dim_num;

  // Check for duplicate dimension names
  if(has_duplicates(dimensions_)) {
    std::string errmsg = "Cannot set dimensions; Duplicate dimension names";
    PRINT_ERROR(errmsg);
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  }

  // Check if a dimension has the same name as an attribute 
  if(intersect(attributes_, dimensions_)) {
    std::string errmsg = 
        "Cannot set dimensions; Attribute name same as dimension name";
    PRINT_ERROR(errmsg);
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  }

  // Success
  return TILEDB_AS_OK;
}

int ArraySchema::set_domain(const void* domain) {
  // Sanity check
  if(domain == NULL) {
    std::string errmsg = "Cannot set domain; Domain not provided";
    PRINT_ERROR(errmsg);
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
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
  if(types_[attribute_num_] == TILEDB_INT32) {
    int* domain_int = (int*) domain_;
    for(int i=0; i<dim_num_; ++i) {
      if(domain_int[2*i] > domain_int[2*i+1]) {
        std::string errmsg = 
            "Cannot set domain; Lower domain bound larger than its "
            "corresponding upper";
        PRINT_ERROR(errmsg);
        tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
        return TILEDB_AS_ERR;
      }
    }
  } else if(types_[attribute_num_] == TILEDB_INT64) {
    int64_t* domain_int64 = (int64_t*) domain_;
    for(int i=0; i<dim_num_; ++i) {
      if(domain_int64[2*i] > domain_int64[2*i+1]) {
        std::string errmsg = 
            "Cannot set domain; Lower domain bound larger than its "
            "corresponding upper";
        PRINT_ERROR(errmsg);
        tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
        return TILEDB_AS_ERR;
      }
    }
  } else if(types_[attribute_num_] == TILEDB_FLOAT32) {
    float* domain_float = (float*) domain_;
    for(int i=0; i<dim_num_; ++i) {
      if(domain_float[2*i] > domain_float[2*i+1]) {
        std::string errmsg = 
            "Cannot set domain; Lower domain bound larger than its "
            "corresponding upper";
        PRINT_ERROR(errmsg);
        tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
        return TILEDB_AS_ERR;
      }
    }
  } else if(types_[attribute_num_] == TILEDB_FLOAT64) {
    double* domain_double = (double*) domain_;
    for(int i=0; i<dim_num_; ++i) {
      if(domain_double[2*i] > domain_double[2*i+1]) {
        std::string errmsg = 
            "Cannot set domain; Lower domain bound larger than its "
            "corresponding upper";
        PRINT_ERROR(errmsg);
        tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
        return TILEDB_AS_ERR;
      }
    }
  } else {
    std::string errmsg = "Cannot set domain; Invalid coordinates type";
    PRINT_ERROR(errmsg);
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  }

  // Success
  return TILEDB_AS_OK;
}

int ArraySchema::set_tile_extents(const void* tile_extents) {
  // Dense arrays must have tile extents
  if(tile_extents == NULL && dense_) {
    std::string errmsg = 
        "Cannot set tile extents; Dense arrays must have tile extents";
    PRINT_ERROR(errmsg);
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  }

  // Free existing tile extends
  if(tile_extents_ != NULL) 
    free(tile_extents_);

  // Set tile extents
  if(tile_extents == NULL) {
    tile_extents_ = NULL;
  } else { 
    size_t tile_extents_size = coords_size();
    tile_extents_ = malloc(tile_extents_size); 
    memcpy(tile_extents_, tile_extents, tile_extents_size);
  }

  // Success
  return TILEDB_AS_OK;
}

int ArraySchema::set_tile_order(int tile_order) {
  // Set tile order
  if(tile_order != TILEDB_ROW_MAJOR &&
     tile_order != TILEDB_COL_MAJOR) {
    std::string errmsg = "Cannot set tile order; Invalid tile order";
    PRINT_ERROR(errmsg); 
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  }
  tile_order_ = tile_order;

  // Success
  return TILEDB_AS_OK;
}

int ArraySchema::set_types(const int* types) {
  // Sanity check
  if(types == NULL) {
    std::string errmsg = "Cannot set types; Types not provided";
    PRINT_ERROR(errmsg);
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  }
  
  // Set attribute types
  for(int i=0; i<attribute_num_; ++i) { 
    if(types[i] != TILEDB_INT32 &&
       types[i] != TILEDB_INT64 &&
       types[i] != TILEDB_FLOAT32 &&
       types[i] != TILEDB_FLOAT64 &&
       types[i] != TILEDB_CHAR) {
      std::string errmsg = "Cannot set types; Invalid type";
      PRINT_ERROR(errmsg);
      tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
      return TILEDB_AS_ERR;
    }
    types_.push_back(types[i]);
  } 

  // Set coordinate type
  if(types[attribute_num_] != TILEDB_INT32 &&
     types[attribute_num_] != TILEDB_INT64 &&
     types[attribute_num_] != TILEDB_FLOAT32 &&
     types[attribute_num_] != TILEDB_FLOAT64) {
    std::string errmsg = "Cannot set types; Invalid type";
    PRINT_ERROR(errmsg);
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  }
  types_.push_back(types[attribute_num_]);

  // Set type sizes
  type_sizes_.resize(attribute_num_ + 1);
  for(int i=0; i < attribute_num_+1; ++i)
    type_sizes_[i] = compute_type_size(i);

  // Set cell sizes
  cell_sizes_.resize(attribute_num_+1);
  for(int i=0; i < attribute_num_+1; ++i) 
    cell_sizes_[i] = compute_cell_size(i);

  // Set the coordinates size
  coords_size_ = cell_sizes_[attribute_num_];

  return TILEDB_AS_OK;
}




/* ****************************** */
/*              MISC              */
/* ****************************** */

template<class T>
int ArraySchema::cell_order_cmp(const T* coords_a, const T* coords_b) const {
  // Check if they are equal
  if(memcmp(coords_a, coords_b, coords_size_) == 0)
    return 0;

  // Check for precedence
  if(cell_order_ == TILEDB_COL_MAJOR) {    // COLUMN-MAJOR
    for(int i=dim_num_-1; i>=0; --i) {
      if(coords_a[i] < coords_b[i])
        return -1;
      else if(coords_a[i] > coords_b[i])
        return 1;
    }
  } else if(cell_order_ == TILEDB_ROW_MAJOR) { // ROW-MAJOR
    for(int i=0; i<dim_num_; ++i) {
      if(coords_a[i] < coords_b[i])
        return -1;
      else if(coords_a[i] > coords_b[i])
        return 1;
    }
  } else if(cell_order_ == TILEDB_HILBERT) {   // HILBERT
    // Check hilbert ids
    int64_t id_a = hilbert_id(coords_a);
    int64_t id_b = hilbert_id(coords_b);

    if(id_a < id_b)
      return -1;
    else if(id_a > id_b)
      return 1;

    // Hilbert ids match - check coordinates in row-major order
    for(int i=0; i<dim_num_; ++i) {
      if(coords_a[i] < coords_b[i])
        return -1;
      else if(coords_a[i] > coords_b[i])
        return 1;
    }
  } else {  // Invalid cell order
    assert(0);
  }

  // The program should never reach this point
  assert(0);
  return 0;
}

void ArraySchema::expand_domain(void* domain) const {
  if(types_[attribute_num_] == TILEDB_INT32)
    expand_domain<int>(static_cast<int*>(domain));
  else if(types_[attribute_num_] == TILEDB_INT64)
    expand_domain<int64_t>(static_cast<int64_t*>(domain));
}

template<class T>
void ArraySchema::expand_domain(T* domain) const {
  // Applicable only to regular tiles
  if(tile_extents_ == NULL)
    return;

  const T* tile_extents = static_cast<const T*>(tile_extents_); 
  const T* array_domain = static_cast<const T*>(domain_); 

  for(int i=0; i<dim_num_; ++i) {
    domain[2*i] = 
        ((domain[2*i] - array_domain[2*i]) / tile_extents[i] * 
        tile_extents[i]) + array_domain[2*i];
    domain[2*i+1] = 
        ((domain[2*i+1] - array_domain[2*i]) / tile_extents[i] + 1) * 
        tile_extents[i] - 1 + array_domain[2*i];
  }
}

template<class T>
int64_t ArraySchema::get_cell_pos(const T* coords) const {
  // Applicable only to dense arrays
  if(!dense_) {
    std::string errmsg = 
        "Cannot get cell position; Invalid array type";
    PRINT_ERROR(errmsg);
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  }

  // Invoke the proper function based on the cell order
  if(cell_order_ == TILEDB_ROW_MAJOR) {
    return get_cell_pos_row(coords);
  } else if(cell_order_ == TILEDB_COL_MAJOR) {
    return get_cell_pos_col(coords);
  } else {
    std::string errmsg = "Cannot get cell position; Invalid cell order";
    PRINT_ERROR(errmsg);
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  }
}

template<class T>
void ArraySchema::get_next_cell_coords(
    const T* domain,
    T* cell_coords,
    bool& coords_retrieved) const {
  // Sanity check
  assert(dense_);

  // Invoke the proper function based on the tile order
  if(cell_order_ == TILEDB_ROW_MAJOR)
    get_next_cell_coords_row(domain, cell_coords, coords_retrieved);
  else if(cell_order_ == TILEDB_COL_MAJOR)
    get_next_cell_coords_col(domain, cell_coords, coords_retrieved);
  else  // Sanity check
    assert(0);
}

template<class T>
void ArraySchema::get_next_tile_coords(
    const T* domain,
    T* tile_coords) const {
  // Sanity check
  assert(dense_);

  // Invoke the proper function based on the tile order
  if(tile_order_ == TILEDB_ROW_MAJOR)
    get_next_tile_coords_row(domain, tile_coords);
  else if(tile_order_ == TILEDB_COL_MAJOR)
    get_next_tile_coords_col(domain, tile_coords);
  else  // Sanity check
    assert(0);
}

template<class T>
void ArraySchema::get_previous_cell_coords(
    const T* domain,
    T* cell_coords) const {
  // Sanity check
  assert(dense_);

  // Invoke the proper function based on the tile order
  if(cell_order_ == TILEDB_ROW_MAJOR)
    get_previous_cell_coords_row(domain, cell_coords);
  else if(cell_order_ == TILEDB_COL_MAJOR)
    get_previous_cell_coords_col(domain, cell_coords);
  else  // Sanity check
    assert(0);
}

template<class T>
void ArraySchema::get_subarray_tile_domain(
    const T* subarray,
    T* tile_domain,
    T* subarray_tile_domain) const {
  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  // Get tile domain
  T tile_num; // Per dimension
  for(int i=0; i<dim_num_; ++i) {
    tile_num = ceil(double(domain[2*i+1] - domain[2*i] + 1) / tile_extents[i]);
    tile_domain[2*i] = 0;
    tile_domain[2*i+1] = tile_num - 1;
  }

  // Calculate subarray in tile domain
  for(int i=0; i<dim_num_; ++i) {
    subarray_tile_domain[2*i] = 
        std::max((subarray[2*i] - domain[2*i]) / tile_extents[i],
            tile_domain[2*i]); 
    subarray_tile_domain[2*i+1] = 
        std::min((subarray[2*i+1] - domain[2*i]) / tile_extents[i],
            tile_domain[2*i+1]); 
  }
}

template<class T>
int64_t ArraySchema::get_tile_pos(const T* tile_coords) const {
  // Sanity check
  assert(tile_extents_);

  // Invoke the proper function based on the tile order
  if(tile_order_ == TILEDB_ROW_MAJOR) {
    return get_tile_pos_row(tile_coords);
  } else if(tile_order_ == TILEDB_COL_MAJOR) {
    return get_tile_pos_col(tile_coords);
  } else { // Sanity check 
    assert(0);
    std::string errmsg = "Cannot get tile position; Invalid tile order";
    PRINT_ERROR(errmsg);
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  }
}

template<class T>
int64_t ArraySchema::get_tile_pos(
    const T* domain,
    const T* tile_coords) const {
  // Sanity check
  assert(tile_extents_);

  // Invoke the proper function based on the tile order
  if(tile_order_ == TILEDB_ROW_MAJOR) {
    return get_tile_pos_row(domain, tile_coords);
  } else if(tile_order_ == TILEDB_COL_MAJOR) {
    return get_tile_pos_col(domain, tile_coords);
  } else { // Sanity check 
    assert(0);
    std::string errmsg = "Cannot get tile position; Invalid tile order";
    PRINT_ERROR(errmsg);
    tiledb_as_errmsg = TILEDB_AS_ERRMSG + errmsg;
    return TILEDB_AS_ERR;
  }
}

template<class T>
void ArraySchema::get_tile_subarray(
    const T* tile_coords,
    T* tile_subarray) const {
  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  for(int i=0; i<dim_num_; ++i) {
    tile_subarray[2*i] = tile_coords[i] * tile_extents[i] + domain[2*i];
    tile_subarray[2*i+1] = 
        (tile_coords[i] + 1) * tile_extents[i] - 1 + domain[2*i];
  }
}

template<typename T>
int64_t ArraySchema::hilbert_id(const T* coords) const {
  // For easy reference
  const T* domain = static_cast<const T*>(domain_);

  // Normalize coordinates
  for(int i = 0; i < dim_num_; ++i) 
    coords_for_hilbert_[i] = static_cast<int>(coords[i] - domain[2*i]);

  // Compute Hilber id
  int64_t id;
  hilbert_curve_->coords_to_hilbert(coords_for_hilbert_, id);

  // Return
  return id;
}

template<class T>
int ArraySchema::tile_cell_order_cmp(
    const T* coords_a, 
    const T* coords_b) const {
  // Check tile order
  int tile_cmp = tile_order_cmp(coords_a, coords_b);
  if(tile_cmp)
    return tile_cmp;

  // Check cell order
  return cell_order_cmp(coords_a, coords_b);
}

template<typename T>
inline
int64_t ArraySchema::tile_id(const T* cell_coords) const {
  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  // Trivial case
  if(tile_extents == NULL)
    return 0;

  // Calculate tile coordinates
  T* tile_coords = static_cast<T*>(tile_coords_aux_);
  for(int i=0; i<dim_num_; ++i)
    tile_coords[i] = (cell_coords[i] - domain[2*i]) / tile_extents[i]; 

  int tile_id = get_tile_pos(tile_coords);

  // Return
  return tile_id;
}

template<>
int ArraySchema::tile_order_cmp(
    const int* coords_a, 
    const int* coords_b) const {
  // For easy reference
  int diff; 
  int norm;
  const int* domain = static_cast<const int*>(domain_);
  const int* tile_extents = static_cast<const int*>(tile_extents_);

  // If there are regular tiles, first check tile order
  if(tile_extents_ != NULL) {
    // ROW-MAJOR
    if(tile_order_ == TILEDB_ROW_MAJOR) {
      // Check if the cells are definitely IN the same tile
      for(int i=0; i<dim_num_; ++i) {
        diff = coords_a[i] - coords_b[i];

        if(diff < 0)
          norm = (coords_a[i] - domain[2*i]) % tile_extents[i];
        else if(diff > 0)
          norm = (coords_b[i] - domain[2*i]) % tile_extents[i];

        if(diff < 0 && (norm - diff) >= tile_extents[i])
          return -1;
        else if(diff > 0 && (norm + diff) >= tile_extents[i]) 
          return 1;
      }
    } else { // COLUMN-MAJOR
      // Check if the cells are definitely IN the same tile
      for(int i=dim_num_-1; i>=0; --i) {
        diff = coords_a[i] - coords_b[i];

        if(diff < 0)
          norm = (coords_a[i] - domain[2*i]) % tile_extents[i];
        else if(diff > 0)
          norm = (coords_b[i] - domain[2*i]) % tile_extents[i];

        if(diff < 0 && (norm - diff) >= tile_extents[i])
          return -1;
        else if(diff > 0 && (norm + diff) >= tile_extents[i]) 
          return 1;


        if(diff < 0 && (norm - diff) >= tile_extents[i])
          return -1;
        else if(diff > 0 && (norm + diff) >= tile_extents[i]) 
          return 1;
      }
    }
  }

  // Same tile order
  return 0;
}

template<>
int ArraySchema::tile_order_cmp(
    const int64_t* coords_a, 
    const int64_t* coords_b) const {
  // For easy reference
  int64_t diff; 
  int64_t norm;
  const int64_t* domain = static_cast<const int64_t*>(domain_);
  const int64_t* tile_extents = static_cast<const int64_t*>(tile_extents_);

  // If there are regular tiles, first check tile order
  if(tile_extents_ != NULL) {
    // ROW-MAJOR
    if(tile_order_ == TILEDB_ROW_MAJOR) {
      // Check if the cells are definitely IN the same tile
      for(int i=0; i<dim_num_; ++i) {
        diff = coords_a[i] - coords_b[i];
       
        if(diff < 0)
          norm = (coords_a[i] - domain[2*i]) % tile_extents[i];
        else if(diff > 0)
          norm = (coords_b[i] - domain[2*i]) % tile_extents[i];

        if(diff < 0 && (norm - diff) >= tile_extents[i])
          return -1;
        else if(diff > 0 && (norm + diff) >= tile_extents[i]) 
          return 1;
      }
    } else { // COLUMN-MAJOR
      // Check if the cells are definitely IN the same tile
      for(int i=dim_num_-1; i>=0; --i) {
        diff = coords_a[i] - coords_b[i];

        if(diff < 0)
          norm = (coords_a[i] - domain[2*i]) % tile_extents[i];
        else if(diff > 0)
          norm = (coords_b[i] - domain[2*i]) % tile_extents[i];

        if(diff < 0 && (norm - diff) >= tile_extents[i])
          return -1;
        else if(diff > 0 && (norm + diff) >= tile_extents[i]) 
          return 1;


        if(diff < 0 && (norm - diff) >= tile_extents[i])
          return -1;
        else if(diff > 0 && (norm + diff) >= tile_extents[i]) 
          return 1;
      }
    }
  }

  // Same tile order
  return 0;
}

template<>
int ArraySchema::tile_order_cmp(
    const float* coords_a, 
    const float* coords_b) const {
  // For easy reference
  float diff; 
  float norm, norm_temp;
  const float* domain = static_cast<const float*>(domain_);
  const float* tile_extents = static_cast<const float*>(tile_extents_);

  // If there are regular tiles, first check tile order
  if(tile_extents_ != NULL) {
    // ROW-MAJOR
    if(tile_order_ == TILEDB_ROW_MAJOR) {
      // Check if the cells are definitely IN the same tile
      for(int i=0; i<dim_num_; ++i) {
        diff = coords_a[i] - coords_b[i];

        if(diff < 0) {
          norm_temp = coords_a[i];
          do {
            norm = norm_temp;
            norm_temp -= tile_extents[i];
          } while(norm_temp >= domain[2*i]);
        } else if(diff > 0) {
          norm_temp = coords_b[i];
          do {
            norm = norm_temp;
            norm_temp -= tile_extents[i];
          } while(norm_temp >= domain[2*i]);
        }

        if(diff < 0 && (norm - diff) >= tile_extents[i])
          return -1;
        else if(diff > 0 && (norm + diff) >= tile_extents[i]) 
          return 1;
      }
    } else { // COLUMN-MAJOR
      // Check if the cells are definitely IN the same tile
      for(int i=dim_num_-1; i>=0; --i) {
        diff = coords_a[i] - coords_b[i];

        if(diff < 0) {
          norm_temp = coords_a[i];
          do {
            norm = norm_temp;
            norm_temp -= tile_extents[i];
          } while(norm_temp >= domain[2*i]);
        } else if(diff > 0) {
          norm_temp = coords_b[i];
          do {
            norm = norm_temp;
            norm_temp -= tile_extents[i];
          } while(norm_temp >= domain[2*i]);
        }

        if(diff < 0 && (norm - diff) >= tile_extents[i])
          return -1;
        else if(diff > 0 && (norm + diff) >= tile_extents[i]) 
          return 1;
      }
    }
  }

  // Same tile order
  return 0;
}

template<>
int ArraySchema::tile_order_cmp(
    const double* coords_a, 
    const double* coords_b) const {
  // For easy reference
  double diff; 
  double norm, norm_temp;
  const double* domain = static_cast<const double*>(domain_);
  const double* tile_extents = static_cast<const double*>(tile_extents_);

  // If there are regular tiles, first check tile order
  if(tile_extents_ != NULL) {
    // ROW-MAJOR
    if(tile_order_ == TILEDB_ROW_MAJOR) {
      // Check if the cells are definitely IN the same tile
      for(int i=0; i<dim_num_; ++i) {
        diff = coords_a[i] - coords_b[i];

        if(diff < 0) {
          norm_temp = coords_a[i];
          do {
            norm = norm_temp;
            norm_temp -= tile_extents[i];
          } while(norm_temp >= domain[2*i]);
        } else if(diff > 0) {
          norm_temp = coords_b[i];
          do {
            norm = norm_temp;
            norm_temp -= tile_extents[i];
          } while(norm_temp >= domain[2*i]);
        }

        if(diff < 0 && (norm - diff) >= tile_extents[i])
          return -1;
        else if(diff > 0 && (norm + diff) >= tile_extents[i]) 
          return 1;
      }
    } else { // COLUMN-MAJOR
      // Check if the cells are definitely IN the same tile
      for(int i=dim_num_-1; i>=0; --i) {
        diff = coords_a[i] - coords_b[i];

        if(diff < 0) {
          norm_temp = coords_a[i];
          do {
            norm = norm_temp;
            norm_temp -= tile_extents[i];
          } while(norm_temp >= domain[2*i]);
        } else if(diff > 0) {
          norm_temp = coords_b[i];
          do {
            norm = norm_temp;
            norm_temp -= tile_extents[i];
          } while(norm_temp >= domain[2*i]);
        }

        if(diff < 0 && (norm - diff) >= tile_extents[i])
          return -1;
        else if(diff > 0 && (norm + diff) >= tile_extents[i]) 
          return 1;
      }
    }
  }

  // Same tile order
  return 0;
}



/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

// ===== FORMAT =====
// array_name_size(int)
//     array_name(string)
// dense(bool)
// tile_order(char)
// cell_order(char)
// capacity(int64_t)
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
// cell_val_num#1(int) cell_val_num#2(int) ... 
// compression#1(char) compression#2(char) ...
size_t ArraySchema::compute_bin_size() const {
  // Initialization
  size_t bin_size = 0;

  // Size for array_name_ 
  bin_size += sizeof(int) + array_name_.size();
  // Size for dense_
  bin_size += sizeof(bool);
  // Size for tile_order_ and cell_order_
  bin_size += 2 * sizeof(char);
  // Size for capacity_ 
  bin_size += sizeof(int64_t);
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
  // Size for cell_val_num_
  bin_size += attribute_num_ * sizeof(int);
  // Size for compression_
  bin_size += (attribute_num_+1) * sizeof(char);

  return bin_size;
}

void ArraySchema::compute_cell_num_per_tile() { 
  //  Meaningful only for dense arrays
  if(!dense_)
    return;

  // Invoke the proper templated function
  int coords_type = types_[attribute_num_];
  if(coords_type == TILEDB_INT32) 
    compute_cell_num_per_tile<int>();
  else if(coords_type == TILEDB_INT64) 
    compute_cell_num_per_tile<int64_t>();
  else   // Sanity check
    assert(0); 
}

template<class T>
void ArraySchema::compute_cell_num_per_tile() {
  const T* tile_extents = static_cast<const T*>(tile_extents_);
  cell_num_per_tile_ = 1;

  for(int i=0; i<dim_num_; ++i)
    cell_num_per_tile_ *= tile_extents[i]; 
}

size_t ArraySchema::compute_cell_size(int i) const {
  assert(i>= 0 && i <= attribute_num_);

  // Variable-sized cell
  if(i<attribute_num_ && cell_val_num_[i] == TILEDB_VAR_NUM)
    return TILEDB_VAR_SIZE;

  // Fixed-sized cell
  size_t size = 0;
  
  // Attributes
  if(i < attribute_num_) {
    if(types_[i] == TILEDB_CHAR)
      size = cell_val_num_[i] * sizeof(char);
    else if(types_[i] == TILEDB_INT32)
      size = cell_val_num_[i] * sizeof(int);
    else if(types_[i] == TILEDB_INT64)
      size = cell_val_num_[i] * sizeof(int64_t);
    else if(types_[i] == TILEDB_FLOAT32)
      size = cell_val_num_[i] * sizeof(float);
    else if(types_[i] == TILEDB_FLOAT64)
      size = cell_val_num_[i] * sizeof(double);
  } else { // Coordinates
    if(types_[i] == TILEDB_INT32)
      size = dim_num_ * sizeof(int);
    else if(types_[i] == TILEDB_INT64)
      size = dim_num_ * sizeof(int64_t);
    else if(types_[i] == TILEDB_FLOAT32)
      size = dim_num_ * sizeof(float);
    else if(types_[i] == TILEDB_FLOAT64)
      size = dim_num_ * sizeof(double);
  }

  return size; 
}

template<class T>
void ArraySchema::compute_hilbert_bits() {
  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  T max_domain_range = 0;
  T domain_range;

  for(int i = 0; i < dim_num_; ++i) { 
    domain_range = domain[2*i+1] - domain[2*i] + 1;
    if(max_domain_range < domain_range)
      max_domain_range = domain_range;
  }

  hilbert_bits_ = ceil(log2(int64_t(max_domain_range+0.5)));
}

void ArraySchema::compute_tile_domain() {
  // For easy reference 
  int coords_type = types_[attribute_num_];

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32)
    compute_tile_domain<int>();
  else if(coords_type == TILEDB_INT64)
    compute_tile_domain<int64_t>();
  else if(coords_type == TILEDB_FLOAT32)
    compute_tile_domain<float>();
  else if(coords_type == TILEDB_FLOAT64)
    compute_tile_domain<double>();
}

template<class T>
void ArraySchema::compute_tile_domain() {
  if(tile_extents_ == NULL)
    return;  

  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  // Allocate space for the tile domain
  assert(tile_domain_ == NULL);
  tile_domain_ = malloc(2*dim_num_*sizeof(T));

  // For easy reference
  T* tile_domain = static_cast<T*>(tile_domain_);
  T tile_num; // Per dimension

  // Calculate tile domain
  for(int i=0; i<dim_num_; ++i) {
    tile_num = ceil(double(domain[2*i+1] - domain[2*i] + 1) / tile_extents[i]);
    tile_domain[2*i] = 0;
    tile_domain[2*i+1] = tile_num - 1;
  }
}

void ArraySchema::compute_tile_offsets() {
  // Invoke the proper templated function
  if(types_[attribute_num_] == TILEDB_INT32) {
    compute_tile_offsets<int>();
  } else if(types_[attribute_num_] == TILEDB_INT64) {
    compute_tile_offsets<int64_t>();
  } else if(types_[attribute_num_] == TILEDB_FLOAT32) {
    compute_tile_offsets<float>();
  } else if(types_[attribute_num_] == TILEDB_FLOAT64) {
    compute_tile_offsets<double>();
  } else { // The program should never reach this point
    assert(0);
  }
}

template<class T>
void ArraySchema::compute_tile_offsets() {
  // Applicable only to non-NULL space tiles
  if(tile_extents_ == NULL)
    return;

  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  const T* tile_extents = static_cast<const T*>(tile_extents_);
  int64_t tile_num; // Per dimension

  // Calculate tile offsets for column-major tile order
  tile_offsets_col_.push_back(1);
  for(int i=1; i<dim_num_; ++i) {
    tile_num = (domain[2*(i-1)+1] - 
                domain[2*(i-1)] + 1) / tile_extents[i-1];
    tile_offsets_col_.push_back(tile_offsets_col_.back() * tile_num);
  }
  
  // Calculate tile offsets for row-major tile order
  tile_offsets_row_.push_back(1);
  for(int i=dim_num_-2; i>=0; --i) {
    tile_num = (domain[2*(i+1)+1] - 
                domain[2*(i+1)] + 1) / tile_extents[i+1];
    tile_offsets_row_.push_back(tile_offsets_row_.back() * tile_num);
  }
  std::reverse(tile_offsets_row_.begin(), tile_offsets_row_.end());
}

size_t ArraySchema::compute_type_size(int i) const {
  // Sanity check
  assert(i>= 0 && i <= attribute_num_);

  if(types_[i] == TILEDB_CHAR) {
    return sizeof(char);
  } else if(types_[i] == TILEDB_INT32) {
    return sizeof(int);
  } else if(types_[i] == TILEDB_INT64) {
    return sizeof(int64_t);
  } else if(types_[i] == TILEDB_FLOAT32) {
    return sizeof(float);
  } else if(types_[i] == TILEDB_FLOAT64) {
    return sizeof(double);
  } else { // The program should never reach this point
    assert(0);
    return 0;
  }
}

template<class T>
int64_t ArraySchema::get_cell_pos_col(const T* coords) const {
  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  // Calculate cell offsets
  int64_t cell_num; // Per dimension
  std::vector<int64_t> cell_offsets;
  cell_offsets.push_back(1);
  for(int i=1; i<dim_num_; ++i) {
    cell_num = tile_extents[i-1]; 
    cell_offsets.push_back(cell_offsets.back() * cell_num);
  }
 
  // Calculate position
  T coords_norm; // Normalized coordinates inside the tile
  int64_t pos = 0;
  for(int i=0; i<dim_num_; ++i) { 
    coords_norm = (coords[i] - domain[2*i]);
    coords_norm -=  (coords_norm / tile_extents[i]) * tile_extents[i];
    pos += coords_norm * cell_offsets[i];
  }

  // Return
  return pos;
}

template<class T>
int64_t ArraySchema::get_cell_pos_row(const T* coords) const {
  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  // Calculate cell offsets
  int64_t cell_num; // Per dimension
  std::vector<int64_t> cell_offsets;
  cell_offsets.push_back(1);
  for(int i=dim_num_-2; i>=0; --i) {
    cell_num = tile_extents[i+1];
    cell_offsets.push_back(cell_offsets.back() * cell_num);
  }
  std::reverse(cell_offsets.begin(), cell_offsets.end());
 
  // Calculate position
  T coords_norm; // Normalized coordinates inside the tile
  int64_t pos = 0;
  for(int i=0; i<dim_num_; ++i) { 
    coords_norm = (coords[i] - domain[2*i]);
    coords_norm -=  (coords_norm / tile_extents[i]) * tile_extents[i];
    pos += coords_norm * cell_offsets[i];
  }

  // Return
  return pos;
}

template<class T>
void ArraySchema::get_next_cell_coords_col(
    const T* domain,
    T* cell_coords,
    bool& coords_retrieved) const {
  int i = 0;
  ++cell_coords[i];

  while(i < dim_num_-1 && cell_coords[i] > domain[2*i+1]) {
    cell_coords[i] = domain[2*i];
    ++cell_coords[++i];
  } 

  if(i == dim_num_-1 && cell_coords[i] > domain[2*i+1])
    coords_retrieved = false;
  else
    coords_retrieved = true; 
}

template<class T>
void ArraySchema::get_next_cell_coords_row(
    const T* domain,
    T* cell_coords,
    bool& coords_retrieved) const {
  int i = dim_num_-1;
  ++cell_coords[i];

  while(i > 0 && cell_coords[i] > domain[2*i+1]) {
    cell_coords[i] = domain[2*i];
    ++cell_coords[--i];
  } 

  if(i == 0 && cell_coords[i] > domain[2*i+1])
    coords_retrieved = false;
  else
    coords_retrieved = true; 
}

template<class T>
void ArraySchema::get_previous_cell_coords_col(
    const T* domain,
    T* cell_coords) const {
  int i = 0;
  --cell_coords[i];

  while(i < dim_num_-1 && cell_coords[i] < domain[2*i]) {
    cell_coords[i] = domain[2*i+1];
    --cell_coords[++i];
  } 
}

template<class T>
void ArraySchema::get_previous_cell_coords_row(
    const T* domain,
    T* cell_coords) const {
  int i = dim_num_-1;
  --cell_coords[i];

  while(i > 0 && cell_coords[i] < domain[2*i]) {
    cell_coords[i] = domain[2*i+1];
    --cell_coords[--i];
  } 
}

template<class T>
void ArraySchema::get_next_tile_coords_col(
    const T* domain,
    T* tile_coords) const {
  int i = 0;
  ++tile_coords[i];

  while(i < dim_num_-1 && tile_coords[i] > domain[2*i+1]) {
    tile_coords[i] = domain[2*i];
    ++tile_coords[++i];
  } 
}

template<class T>
void ArraySchema::get_next_tile_coords_row(
    const T* domain,
    T* tile_coords) const {
  int i = dim_num_-1;
  ++tile_coords[i];

  while(i > 0 && tile_coords[i] > domain[2*i+1]) {
    tile_coords[i] = domain[2*i];
    ++tile_coords[--i];
  } 
}

template<class T>
int64_t ArraySchema::get_tile_pos_col(const T* tile_coords) const {
  // Calculate position
  int64_t pos = 0;
  for(int i=0; i<dim_num_; ++i) 
    pos += tile_coords[i] * tile_offsets_col_[i];

  // Return
  return pos;
}

template<class T>
int64_t ArraySchema::get_tile_pos_col(
    const T* domain,
    const T* tile_coords) const {
  // For easy reference
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  // Calculate tile offsets
  int64_t tile_num; // Per dimension
  std::vector<int64_t> tile_offsets;
  tile_offsets.push_back(1);
  for(int i=1; i<dim_num_; ++i) {
    tile_num = (domain[2*(i-1)+1] - 
                domain[2*(i-1)] + 1) / tile_extents[i-1];
    tile_offsets.push_back(tile_offsets.back() * tile_num);
  }
 
  // Calculate position
  int64_t pos = 0;
  for(int i=0; i<dim_num_; ++i) 
    pos += tile_coords[i] * tile_offsets[i];

  // Return
  return pos;
}

template<class T>
int64_t ArraySchema::get_tile_pos_row(const T* tile_coords) const {
  // Calculate position
  int64_t pos = 0;
  for(int i=0; i<dim_num_; ++i) 
    pos += tile_coords[i] * tile_offsets_row_[i];

  // Return
  return pos;
}

template<class T>
int64_t ArraySchema::get_tile_pos_row(
    const T* domain,
    const T* tile_coords) const {
  // For easy reference
  const T* tile_extents = static_cast<const T*>(tile_extents_);
  
  // Calculate tile offsets
  int64_t tile_num; // Per dimension
  std::vector<int64_t> tile_offsets;
  tile_offsets.push_back(1);
  for(int i=dim_num_-2; i>=0; --i) {
    tile_num = (domain[2*(i+1)+1] - 
                domain[2*(i+1)] + 1) / tile_extents[i+1];
    tile_offsets.push_back(tile_offsets.back() * tile_num);
  }
  std::reverse(tile_offsets.begin(), tile_offsets.end());
 
  // Calculate position
  int64_t pos = 0;
  for(int i=0; i<dim_num_; ++i) 
    pos += tile_coords[i] * tile_offsets[i];

  // Return
  return pos;
}

void ArraySchema::init_hilbert_curve() {
  // Applicable only to Hilbert cell order
  if(cell_order_ != TILEDB_HILBERT) 
    return;

  // Allocate some space for the Hilbert coordinates
  if(coords_for_hilbert_ == NULL)
    coords_for_hilbert_ = new int[dim_num_];

  // Compute Hilbert bits, invoking the proper templated function
  if(types_[attribute_num_] == TILEDB_INT32)
    compute_hilbert_bits<int>();
  else if(types_[attribute_num_] == TILEDB_INT64)
    compute_hilbert_bits<int64_t>();
  else if(types_[attribute_num_] == TILEDB_FLOAT32)
    compute_hilbert_bits<float>();
  else if(types_[attribute_num_] == TILEDB_FLOAT64)
    compute_hilbert_bits<double>();

  // Create new Hilberrt curve
  hilbert_curve_ = new HilbertCurve(hilbert_bits_, dim_num_);
}




// Explicit template instantiations

template int ArraySchema::cell_order_cmp<int>(
    const int* coords_a, 
    const int* coords_b) const;
template int ArraySchema::cell_order_cmp<int64_t>(
    const int64_t* coords_a, 
    const int64_t* coords_b) const;
template int ArraySchema::cell_order_cmp<float>(
    const float* coords_a, 
    const float* coords_b) const;
template int ArraySchema::cell_order_cmp<double>(
    const double* coords_a, 
    const double* coords_b) const;

template int64_t ArraySchema::get_cell_pos<int>(
    const int* coords) const;
template int64_t ArraySchema::get_cell_pos<int64_t>(
    const int64_t* coords) const;
template int64_t ArraySchema::get_cell_pos<float>(
    const float* coords) const;
template int64_t ArraySchema::get_cell_pos<double>(
    const double* coords) const;

template void ArraySchema::get_next_cell_coords<int>(
    const int* domain,
    int* cell_coords,
    bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<int64_t>(
    const int64_t* domain,
    int64_t* cell_coords,
    bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<float>(
    const float* domain,
    float* cell_coords,
    bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<double>(
    const double* domain,
    double* cell_coords,
    bool& coords_retrieved) const;

template void ArraySchema::get_next_tile_coords<int>(
    const int* domain,
    int* tile_coords) const;
template void ArraySchema::get_next_tile_coords<int64_t>(
    const int64_t* domain,
    int64_t* tile_coords) const;
template void ArraySchema::get_next_tile_coords<float>(
    const float* domain,
    float* tile_coords) const;
template void ArraySchema::get_next_tile_coords<double>(
    const double* domain,
    double* tile_coords) const;

template void ArraySchema::get_previous_cell_coords<int>(
    const int* domain,
    int* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<int64_t>(
    const int64_t* domain,
    int64_t* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<float>(
    const float* domain,
    float* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<double>(
    const double* domain,
    double* cell_coords) const;

template void ArraySchema::get_subarray_tile_domain<int>(
    const int* subarray,
    int* tile_domain,
    int* subarray_tile_domain) const;
template void ArraySchema::get_subarray_tile_domain<int64_t>(
    const int64_t* subarray,
    int64_t* tile_domain,
    int64_t* subarray_tile_domain) const;

template int64_t ArraySchema::get_tile_pos<int>(
    const int* domain,
    const int* tile_coords) const;
template int64_t ArraySchema::get_tile_pos<int64_t>(
    const int64_t* domain,
    const int64_t* tile_coords) const;
template int64_t ArraySchema::get_tile_pos<float>(
    const float* domain,
    const float* tile_coords) const;
template int64_t ArraySchema::get_tile_pos<double>(
    const double* domain,
    const double* tile_coords) const;

template void ArraySchema::get_tile_subarray<int>(
    const int* tile_coords,
    int* tile_subarray) const;
template void ArraySchema::get_tile_subarray<int64_t>(
    const int64_t* tile_coords,
    int64_t* tile_subarray) const;

template int64_t ArraySchema::hilbert_id<int>(
    const int* coords) const;
template int64_t ArraySchema::hilbert_id<int64_t>(
    const int64_t* coords) const;
template int64_t ArraySchema::hilbert_id<float>(
    const float* coords) const;
template int64_t ArraySchema::hilbert_id<double>(
    const double* coords) const;

template int ArraySchema::subarray_overlap<int>(
    const int* subarray_a, 
    const int* subarray_b, 
    int* overlap_subarray) const;
template int ArraySchema::subarray_overlap<int64_t>(
    const int64_t* subarray_a, 
    const int64_t* subarray_b, 
    int64_t* overlap_subarray) const;
template int ArraySchema::subarray_overlap<float>(
    const float* subarray_a, 
    const float* subarray_b, 
    float* overlap_subarray) const;
template int ArraySchema::subarray_overlap<double>(
    const double* subarray_a, 
    const double* subarray_b, 
    double* overlap_subarray) const;

template int ArraySchema::tile_cell_order_cmp<int>(
    const int* coords_a, 
    const int* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<int64_t>(
    const int64_t* coords_a, 
    const int64_t* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<float>(
    const float* coords_a, 
    const float* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<double>(
    const double* coords_a, 
    const double* coords_b) const;

template int64_t ArraySchema::tile_id<int>(
    const int* cell_coords) const;
template int64_t ArraySchema::tile_id<int64_t>(
    const int64_t* cell_coords) const;
template int64_t ArraySchema::tile_id<float>(
    const float* cell_coords) const;
template int64_t ArraySchema::tile_id<double>(
    const double* cell_coords) const;

