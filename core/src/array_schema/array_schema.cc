/**
 * @file   array_schema.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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

#include <cinttypes>
#include <cassert>
#include <cmath>
#include <iostream>

#include "array_schema.h"
#include "logger.h"
#include "utils.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArraySchema::ArraySchema() {
  attribute_num_ = 0;
  cell_num_per_tile_ = 0;
  dim_num_ = 0;
  domain_ = nullptr;
  tile_extents_ = nullptr;
  tile_domain_ = nullptr;
  tile_coords_aux_ = nullptr;
  array_uri_ = URI();
  array_type_ = ArrayType::DENSE;
  capacity_ = constants::capacity;
  cell_order_ = Layout::ROW_MAJOR;
  tile_order_ = Layout::ROW_MAJOR;
}

ArraySchema::ArraySchema(const ArraySchema* array_schema) {
  array_uri_ = array_schema->array_uri_;
  array_type_ = array_schema->array_type_;
  attributes_ = array_schema->attributes_;
  attribute_num_ = array_schema->attribute_num();
  capacity_ = array_schema->capacity_;
  cell_num_per_tile_ = array_schema->cell_num_per_tile_;
  cell_order_ = array_schema->cell_order_;
  cell_sizes_ = array_schema->cell_sizes_;
  coords_size_ = array_schema->coords_size_;
  dimensions_ = array_schema->dimensions_;
  dim_num_ = array_schema->dim_num();
  tile_coords_aux_ = malloc(coords_size_ * dim_num_);
  tile_offsets_col_ = array_schema->tile_offsets_col_;
  tile_offsets_row_ = array_schema->tile_offsets_row_;
  tile_order_ = array_schema->tile_order_;
  type_sizes_ = array_schema->type_sizes_;

  if (array_schema->domain_ == nullptr) {
    domain_ = nullptr;
  } else {
    domain_ = malloc(2 * coords_size_);
    memcpy(domain_, array_schema->domain_, 2 * coords_size_);
  }

  if (array_schema->tile_domain_ == nullptr) {
    tile_domain_ = nullptr;
  } else {
    tile_domain_ = malloc(2 * coords_size_);
    memcpy(tile_domain_, array_schema->tile_domain_, 2 * coords_size_);
  }

  if (array_schema->tile_extents_ == nullptr) {
    tile_extents_ = nullptr;
  } else {
    tile_extents_ = malloc(coords_size_);
    memcpy(tile_extents_, array_schema->tile_extents_, coords_size_);
  }
}

ArraySchema::ArraySchema(const URI& uri) {
  cell_num_per_tile_ = 0;
  domain_ = nullptr;
  tile_extents_ = nullptr;
  tile_domain_ = nullptr;
  tile_coords_aux_ = nullptr;
  array_uri_ = uri;
  array_type_ = ArrayType::DENSE;
  capacity_ = constants::capacity;
  cell_order_ = Layout::ROW_MAJOR;
  tile_order_ = Layout::ROW_MAJOR;
}

ArraySchema::~ArraySchema() {
  clear();
}

/* ****************************** */
/*            ACCESSORS           */
/* ****************************** */

const URI& ArraySchema::array_uri() const {
  return array_uri_;
}

ArrayType ArraySchema::array_type() const {
  return array_type_;
}

const Attribute* ArraySchema::attr(unsigned int id) const {
  if (id < attributes_.size())
    return attributes_[id];

  return nullptr;
}

const std::string& ArraySchema::attribute(unsigned int attribute_id) const {
  assert(attribute_id >= 0 && attribute_id <= attribute_num_ + 1);

  // Special case for the search attribute (same as coordinates)
  if (attribute_id == attribute_num_ + 1)
    attribute_id = attribute_num_;

  return attributes_[attribute_id]->name();
}

Status ArraySchema::attribute_id(const std::string& attribute_name, unsigned int* id) const {
  // Special case - coordinates
  if (attribute_name == constants::coords) {
    *id = attribute_num_;
    return Status::Ok();
  }

  for (unsigned int i = 0; i < attribute_num_; ++i) {
    if (attributes_[i]->name() == attribute_name) {
      *id = i;
      return Status::Ok();
    }
  }
  return LOG_STATUS(
      Status::ArraySchemaError("Attribute not found: " + attribute_name));
}

std::vector<std::string> ArraySchema::attribute_names() const {
  std::vector<std::string> names;
  for(auto& attr : attributes_)
   names.emplace_back(attr->name());
  names.emplace_back(constants::coords);
  return names;
}

unsigned int ArraySchema::attribute_num() const {
  return attribute_num_;
}

const std::vector<Attribute*>& ArraySchema::Attributes() const {
  return attributes_;
}

const std::vector<Dimension*>& ArraySchema::Dimensions() const {
  return dimensions_;
}

uint64_t ArraySchema::capacity() const {
  return capacity_;
}

uint64_t ArraySchema::cell_num_per_tile() const {
  // Sanity check
  assert(array_type_ == ArrayType::DENSE);

  return cell_num_per_tile_;
}

Layout ArraySchema::cell_order() const {
  return cell_order_;
}

uint64_t ArraySchema::cell_size(unsigned int attribute_id) const {
  // Special case for the search tile
  if (attribute_id == attribute_num_ + 1)
    attribute_id = attribute_num_;

  return cell_sizes_[attribute_id];
}

unsigned int ArraySchema::cell_val_num(unsigned int attribute_id) const {
  return attributes_[attribute_id]->cell_val_num();
}

Status ArraySchema::check() const {
  if(array_uri_.is_invalid())
    return LOG_STATUS(Status::ArraySchemaError("Array schema check failed; Invalid array URI"));

  if(dim_num_ == 0)
    return LOG_STATUS(Status::ArraySchemaError("Array schema check failed; No dimensions provided"));

  // Success
  return Status::Ok();
}

Compressor ArraySchema::compression(unsigned int attr_id) const {
  assert(attr_id >= 0 && attr_id <= attribute_num_ + 1);

  if (attr_id == attribute_num_ + 1)
    return dimensions_[0]->compressor();

  return attributes_[attr_id]->compressor();
}

int ArraySchema::compression_level(unsigned int attr_id) const {
  assert(attr_id >= 0 && attr_id <= attribute_num_ + 1);

  if (attr_id == attribute_num_ + 1)
    return dimensions_[0]->compression_level();

  return attributes_[attr_id]->compression_level();
}

uint64_t ArraySchema::coords_size() const {
  return coords_size_;
}

Datatype ArraySchema::coords_type() const {
  return dimensions_[0]->type();
}

bool ArraySchema::dense() const {
  return array_type_ == ArrayType ::DENSE;
}

const Dimension* ArraySchema::dim(unsigned int id) const {
  if (id < dimensions_.size())
    return dimensions_[id];

  return nullptr;
}

unsigned int ArraySchema::dim_num() const {
  return dim_num_;
}

const void* ArraySchema::domain() const {
  return domain_;
}

void ArraySchema::dump(FILE* out) const {
  const char* array_type_s = array_type_str(array_type_);
  const char* cell_order_s = layout_str(cell_order_);
  const char* tile_order_s = layout_str(tile_order_);

  fprintf(out, "- Array name: %s\n", array_uri_.to_string().c_str());
  fprintf(out, "- Array type: %s\n", array_type_s);
  fprintf(out, "- Cell order: %s\n", cell_order_s);
  fprintf(out, "- Tile order: %s\n", tile_order_s);
  fprintf(out, "- Capacity: %" PRIu64 "\n", capacity_);

  for (auto& dim : dimensions_) {
    fprintf(out, "\n");
    dim->dump(out);
  }

  for (auto& attr : attributes_) {
    fprintf(out, "\n");
    attr->dump(out);
  }
}

Status ArraySchema::get_attribute_ids(
    const std::vector<std::string>& attributes,
    std::vector<unsigned int>& attribute_ids) const {
  // Initialization
  attribute_ids.clear();
  auto attribute_num = (unsigned int)attributes.size();
  unsigned int id;

  // Get attribute ids
  for (unsigned int i = 0; i < attribute_num; ++i) {
    RETURN_NOT_OK(attribute_id(attributes[i], &id));
    attribute_ids.push_back(id);
  }
  return Status::Ok();
}

bool ArraySchema::is_contained_in_tile_slab_col(const void* range) const {
  Datatype typ = coords_type();
  switch (typ) {
    case Datatype::INT32:
      return is_contained_in_tile_slab_col(static_cast<const int*>(range));
    case Datatype::INT64:
      return is_contained_in_tile_slab_col(static_cast<const int64_t*>(range));
    case Datatype::FLOAT32:
      return is_contained_in_tile_slab_col(static_cast<const float*>(range));
    case Datatype::FLOAT64:
      return is_contained_in_tile_slab_col(static_cast<const double*>(range));
    case Datatype::INT8:
      return is_contained_in_tile_slab_col(static_cast<const int8_t*>(range));
    case Datatype::UINT8:
      return is_contained_in_tile_slab_col(static_cast<const uint8_t*>(range));
    case Datatype::INT16:
      return is_contained_in_tile_slab_col(static_cast<const int16_t*>(range));
    case Datatype::UINT16:
      return is_contained_in_tile_slab_col(static_cast<const uint16_t*>(range));
    case Datatype::UINT32:
      return is_contained_in_tile_slab_col(static_cast<const uint32_t*>(range));
    case Datatype::UINT64:
      return is_contained_in_tile_slab_col(static_cast<const uint64_t*>(range));
    default:
      return false;
  }
}

bool ArraySchema::is_contained_in_tile_slab_row(const void* range) const {
  Datatype typ = coords_type();
  switch (typ) {
    case Datatype::INT32:
      return is_contained_in_tile_slab_row(static_cast<const int*>(range));
    case Datatype::INT64:
      return is_contained_in_tile_slab_row(static_cast<const int64_t*>(range));
    case Datatype::FLOAT32:
      return is_contained_in_tile_slab_row(static_cast<const float*>(range));
    case Datatype::FLOAT64:
      return is_contained_in_tile_slab_row(static_cast<const double*>(range));
    case Datatype::INT8:
      return is_contained_in_tile_slab_row(static_cast<const int8_t*>(range));
    case Datatype::UINT8:
      return is_contained_in_tile_slab_row(static_cast<const uint8_t*>(range));
    case Datatype::INT16:
      return is_contained_in_tile_slab_row(static_cast<const int16_t*>(range));
    case Datatype::UINT16:
      return is_contained_in_tile_slab_row(static_cast<const uint16_t*>(range));
    case Datatype::UINT32:
      return is_contained_in_tile_slab_row(static_cast<const uint32_t*>(range));
    case Datatype::UINT64:
      return is_contained_in_tile_slab_row(static_cast<const uint64_t*>(range));
    default:
      return false;
  }
}

// ===== FORMAT =====
// array_uri_size (unsigned int)
// array_uri (string)
// array_type (char)
// tile_order (char)
// cell_order (char)
// capacity (uint64_t)
// dim_num (unsigned int)
//   dimension #1
//   dimension #2
//   ...
// attribute_num (unsigned int)
//   attribute #1
//   attribute #2
//   ...
Status ArraySchema::serialize(Buffer* buff) const {
  // Write array uri
  auto array_uri_size = (unsigned int)array_uri_.to_string().size();
  RETURN_NOT_OK(buff->write(&array_uri_size, sizeof(unsigned int)));
  RETURN_NOT_OK(buff->write(array_uri_.to_string().c_str(), array_uri_size));

  // Write array type
  auto array_type = (char) array_type_;
  RETURN_NOT_OK(buff->write(&array_type, sizeof(char)));

  // Write tile and cell order
  auto tile_order = (char) tile_order_;
  RETURN_NOT_OK(buff->write(&tile_order, sizeof(char)));
  auto cell_order = (char) cell_order_;
  RETURN_NOT_OK(buff->write(&cell_order, sizeof(char)));

  // Write capacity
  RETURN_NOT_OK(buff->write(&capacity_, sizeof(uint64_t)));

  // Write dimensions
  RETURN_NOT_OK(buff->write(&dim_num_, sizeof(unsigned int)));
  for(auto& dim : dimensions_)
    RETURN_NOT_OK(dim->serialize(buff));

  // Write attributes
  RETURN_NOT_OK(buff->write(&attribute_num_, sizeof(unsigned int)));
  for(auto& attr : attributes_)
    RETURN_NOT_OK(attr->serialize(buff));

  return Status::Ok();
}

template <class T>
unsigned int ArraySchema::subarray_overlap(
    const T* subarray_a, const T* subarray_b, T* overlap_subarray) const {
  // Get overlap range
  for (unsigned int i = 0; i < dim_num_; ++i) {
    overlap_subarray[2 * i] = MAX(subarray_a[2 * i], subarray_b[2 * i]);
    overlap_subarray[2 * i + 1] =
        MIN(subarray_a[2 * i + 1], subarray_b[2 * i + 1]);
  }

  // Check overlap
  unsigned int overlap = 1;
  for (unsigned int i = 0; i < dim_num_; ++i) {
    if (overlap_subarray[2 * i] > subarray_b[2 * i + 1] ||
        overlap_subarray[2 * i + 1] < subarray_b[2 * i]) {
      overlap = 0;
      break;
    }
  }

  // Check partial overlap
  if (overlap == 1) {
    for (unsigned int i = 0; i < dim_num_; ++i) {
      if (overlap_subarray[2 * i] != subarray_b[2 * i] ||
          overlap_subarray[2 * i + 1] != subarray_b[2 * i + 1]) {
        overlap = 2;
        break;
      }
    }
  }

  // Check contig overlap
  if (overlap == 2  && dim_num_ > 1) {
    overlap = 3;
    if (cell_order_ == Layout::ROW_MAJOR) {  // Row major
      for (unsigned int i = 1; i < dim_num_; ++i) {
        if (overlap_subarray[2 * i] != subarray_b[2 * i] ||
            overlap_subarray[2 * i + 1] != subarray_b[2 * i + 1]) {
          overlap = 2;
          break;
        }
      }
    } else if (cell_order_ == Layout::COL_MAJOR) {  // Column major
      for (unsigned int i = dim_num_ - 2; ; --i) {
        if (overlap_subarray[2 * i] != subarray_b[2 * i] ||
            overlap_subarray[2 * i + 1] != subarray_b[2 * i + 1]) {
          overlap = 2;
          break;
        }
        if(i == 0)
          break;
      }
    }
  }

  // Return
  return overlap;
}

const void* ArraySchema::tile_extents() const {
  return tile_extents_;
}

uint64_t ArraySchema::tile_num() const {
  // Invoke the proper template function
  Datatype typ = coords_type();
  switch (typ) {
    case Datatype::INT32:
      return tile_num<int>();
    case Datatype::INT64:
      return tile_num<int64_t>();
    case Datatype::INT8:
      return tile_num<int8_t>();
    case Datatype::UINT8:
      return tile_num<uint8_t>();
    case Datatype::INT16:
      return tile_num<int16_t>();
    case Datatype::UINT16:
      return tile_num<uint16_t>();
    case Datatype::UINT32:
      return tile_num<uint32_t>();
    case Datatype::UINT64:
      return tile_num<uint64_t>();
    case Datatype::CHAR:
      assert(false);
          return 0;
    case Datatype::FLOAT32:
      assert(false);
      return 0;
    case Datatype::FLOAT64:
      assert(false);
      return 0;
  }
}

template <class T>
uint64_t ArraySchema::tile_num() const {
  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  uint64_t ret = 1;
  for (unsigned int i = 0; i < dim_num_; ++i)
    ret *= (domain[2 * i + 1] - domain[2 * i] + 1) / tile_extents[i];

  return ret;
}

uint64_t ArraySchema::tile_num(const void* range) const {
  // Invoke the proper template function
  Datatype typ = coords_type();
  switch (typ) {
    case Datatype::INT32:
      return tile_num<int>(static_cast<const int*>(range));
    case Datatype::INT64:
      return tile_num<int64_t>(static_cast<const int64_t*>(range));
    case Datatype::INT8:
      return tile_num<int8_t>(static_cast<const int8_t*>(range));
    case Datatype::UINT8:
      return tile_num<uint8_t>(static_cast<const uint8_t*>(range));
    case Datatype::INT16:
      return tile_num<int16_t>(static_cast<const int16_t*>(range));
    case Datatype::UINT16:
      return tile_num<uint16_t>(static_cast<const uint16_t*>(range));
    case Datatype::UINT32:
      return tile_num<uint32_t>(static_cast<const uint32_t*>(range));
    case Datatype::UINT64:
      return tile_num<uint64_t>(static_cast<const uint64_t*>(range));
    case Datatype::CHAR:
      assert(false);
      return 0;
    case Datatype::FLOAT32:
      assert(false);
      return 0;
    case Datatype::FLOAT64:
      assert(false);
      return 0;
  }
}

template <class T>
uint64_t ArraySchema::tile_num(const T* range) const {
  // For easy reference
  auto tile_extents = static_cast<const T*>(tile_extents_);
  auto domain = static_cast<const T*>(domain_);

  uint64_t ret = 1;
  uint64_t start, end;
  for (unsigned int i = 0; i < dim_num_; ++i) {
    start = (range[2 * i] - domain[2 * i]) / tile_extents[i];
    end = (range[2 * i + 1] - domain[2 * i]) / tile_extents[i];
    ret *= (end - start + 1);
  }

  return ret;
}

Layout ArraySchema::tile_order() const {
  return tile_order_;
}

uint64_t ArraySchema::tile_slab_col_cell_num(const void* subarray) const {
  // Invoke the proper templated function
  Datatype typ = coords_type();
  switch (typ) {
    case Datatype::INT32:
      return tile_slab_col_cell_num(static_cast<const int*>(subarray));
    case Datatype::INT64:
      return tile_slab_col_cell_num(static_cast<const int64_t*>(subarray));
    case Datatype::FLOAT32:
      return tile_slab_col_cell_num(static_cast<const float*>(subarray));
    case Datatype::FLOAT64:
      return tile_slab_col_cell_num(static_cast<const double*>(subarray));
    case Datatype::INT8:
      return tile_slab_col_cell_num(static_cast<const int8_t*>(subarray));
    case Datatype::UINT8:
      return tile_slab_col_cell_num(static_cast<const uint8_t*>(subarray));
    case Datatype::INT16:
      return tile_slab_col_cell_num(static_cast<const int16_t*>(subarray));
    case Datatype::UINT16:
      return tile_slab_col_cell_num(static_cast<const uint16_t*>(subarray));
    case Datatype::UINT32:
      return tile_slab_col_cell_num(static_cast<const uint32_t*>(subarray));
    case Datatype::UINT64:
      return tile_slab_col_cell_num(static_cast<const uint64_t*>(subarray));
    case Datatype::CHAR:
      assert(false);
      return 0;
  }
}

uint64_t ArraySchema::tile_slab_row_cell_num(const void* subarray) const {
  // Invoke the proper templated function
  Datatype typ = coords_type();
  switch (typ) {
    case Datatype::INT32:
      return tile_slab_row_cell_num(static_cast<const int*>(subarray));
    case Datatype::INT64:
      return tile_slab_row_cell_num(static_cast<const int64_t*>(subarray));
    case Datatype::FLOAT32:
      return tile_slab_row_cell_num(static_cast<const float*>(subarray));
    case Datatype::FLOAT64:
      return tile_slab_row_cell_num(static_cast<const double*>(subarray));
    case Datatype::INT8:
      return tile_slab_row_cell_num(static_cast<const int8_t*>(subarray));
    case Datatype::UINT8:
      return tile_slab_row_cell_num(static_cast<const uint8_t*>(subarray));
    case Datatype::INT16:
      return tile_slab_row_cell_num(static_cast<const int16_t*>(subarray));
    case Datatype::UINT16:
      return tile_slab_row_cell_num(static_cast<const uint16_t*>(subarray));
    case Datatype::UINT32:
      return tile_slab_row_cell_num(static_cast<const uint32_t*>(subarray));
    case Datatype::UINT64:
      return tile_slab_row_cell_num(static_cast<const uint64_t*>(subarray));
    case Datatype::CHAR:
      assert(false);
          return 0;
  }
}

Datatype ArraySchema::type(unsigned int i) const {
  if (i > attribute_num_) {
    LOG_ERROR("Cannot retrieve type; Invalid attribute id");
    assert(0);
  }
  return attributes_[i]->type();
}

uint64_t ArraySchema::type_size(unsigned int i) const {
  assert(i <= attribute_num_);

  return type_sizes_[i];
}

bool ArraySchema::var_size(unsigned int attribute_id) const {
  return cell_sizes_[attribute_id] == constants::var_size;
}

/* ****************************** */
/*             MUTATORS           */
/* ****************************** */

void ArraySchema::add_attribute(const Attribute* attr) {
  attributes_.emplace_back(new Attribute(attr));
  ++attribute_num_;
}

void ArraySchema::add_dimension(const Dimension* dim) {
  dimensions_.emplace_back(new Dimension(dim));
  ++dim_num_;
}

// ===== FORMAT =====
// array_uri_size (unsigned int)
// array_uri (string)
// array_type (char)
// tile_order (char)
// cell_order (char)
// capacity (uint64_t)
// dim_num (unsigned int)
//   dimension #1
//   dimension #2
//   ...
// attribute_num (unsigned int)
//   attribute #1
//   attribute #2
//   ...
Status ArraySchema::deserialize(ConstBuffer* buff) {
  // Load array uri
  unsigned int array_uri_size;
  RETURN_NOT_OK(buff->read(&array_uri_size, sizeof(unsigned int)));
  std::string array_uri_str;
  array_uri_str.resize(array_uri_size);
  RETURN_NOT_OK(buff->read(&array_uri_str[0], array_uri_size));
  array_uri_ = URI(array_uri_str);

  // Load array type
  char array_type;
  RETURN_NOT_OK(buff->read(&array_type, sizeof(char)));
  array_type_ = (ArrayType) array_type;

  // Load tile order
  char tile_order;
  RETURN_NOT_OK(buff->read(&tile_order, sizeof(char)));
  tile_order_ = (Layout) tile_order;

  // Load cell order
  char cell_order;
  RETURN_NOT_OK(buff->read(&cell_order, sizeof(char)));
  cell_order_ = (Layout) cell_order;

  // Load capacity
  RETURN_NOT_OK(buff->read(&capacity_, sizeof(uint64_t)));

  // Load dimensions
  RETURN_NOT_OK(buff->read(&dim_num_, sizeof(unsigned int)));
  for(unsigned int i=0; i<dim_num_; ++i) {
    auto dim = new Dimension();
    dim->deserialize(buff);
    dimensions_.emplace_back(dim);
  }

  // Load attributes
  RETURN_NOT_OK(buff->read(&attribute_num_, sizeof(unsigned int)));
  for(unsigned int i=0; i<attribute_num_; ++i) {
    auto attr = new Attribute();
    attr->deserialize(buff);
    attributes_.emplace_back(attr);
  }

  // Initialize the rest of the object members
  RETURN_NOT_OK(init());

  // Success
  return Status::Ok();
}

Status ArraySchema::init() {
  // Perform check of all members
  RETURN_NOT_OK(check());

  // Set cell sizes
  cell_sizes_.resize(attribute_num_ + 1);
  for (unsigned int i = 0; i <= attribute_num_; ++i)
    cell_sizes_[i] = compute_cell_size(i);

  // Set domain
  uint64_t coord_size = datatype_size(coords_type());
  coords_size_ = dim_num_ * coord_size;
  if (domain_ != nullptr)
    free(domain_);
  domain_ = malloc(dim_num_ * 2 * coord_size);
  auto domain = (char*)domain_;
  for (unsigned int i = 0; i < dim_num_; ++i) {
    memcpy(
        domain + i * 2 * coord_size, dimensions_[i]->domain(), 2 * coord_size);
  }

  // Set tile extents
  if (tile_extents_ != nullptr)
    free(tile_extents_);
  if (dimensions_[0]->tile_extent() == nullptr) {
    tile_extents_ = nullptr;
  } else {
    tile_extents_ = malloc(dim_num_ * coord_size);
    auto tile_extents = (char*)tile_extents_;
    for (unsigned int i = 0; i < dim_num_; ++i) {
      memcpy(
          tile_extents + i * coord_size,
          dimensions_[i]->tile_extent(),
          coord_size);
    }
  }

  // Compute number of cells per tile
  compute_cell_num_per_tile();

  // Compute tile domain
  compute_tile_domain();

  // Compute tile offsets
  compute_tile_offsets();

  // Initialize static auxiliary variables
  if (tile_coords_aux_ != nullptr)
    free(tile_coords_aux_);
  tile_coords_aux_ = malloc(coords_size_ * dim_num_);

  // Success
  return Status::Ok();
}

void ArraySchema::set_array_type(ArrayType array_type) {
  array_type_ = array_type;
}

void ArraySchema::set_capacity(uint64_t capacity) {
  capacity_ = capacity;
}

void ArraySchema::set_cell_order(Layout cell_order) {
  cell_order_ = cell_order;
}

void ArraySchema::set_tile_order(Layout tile_order) {
  tile_order_ = tile_order;
}

/* ****************************** */
/*              MISC              */
/* ****************************** */

template <class T>
int ArraySchema::cell_order_cmp(const T* coords_a, const T* coords_b) const {
  // Check if they are equal
  if (memcmp(coords_a, coords_b, coords_size_) == 0)
    return 0;

  // Check for precedence
  if (cell_order_ == Layout::COL_MAJOR) {  // COLUMN-MAJOR
    for (unsigned int i = dim_num_ - 1; ; --i) {
      if (coords_a[i] < coords_b[i])
        return -1;
      if (coords_a[i] > coords_b[i])
        return 1;
      if(i == 0)
        break;
    }
  } else if (cell_order_ == Layout::ROW_MAJOR) {  // ROW-MAJOR
    for (unsigned int i = 0; i < dim_num_; ++i) {
      if (coords_a[i] < coords_b[i])
        return -1;
      if (coords_a[i] > coords_b[i])
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
  Datatype typ = coords_type();
  switch (typ) {
    case Datatype::INT32:
      expand_domain<int>(static_cast<int*>(domain));
      break;
    case Datatype::INT64:
      expand_domain<int64_t>(static_cast<int64_t*>(domain));
      break;
    case Datatype::INT8:
      expand_domain<int8_t>(static_cast<int8_t*>(domain));
      break;
    case Datatype::UINT8:
      expand_domain<uint8_t>(static_cast<uint8_t*>(domain));
      break;
    case Datatype::INT16:
      expand_domain<int16_t>(static_cast<int16_t*>(domain));
      break;
    case Datatype::UINT16:
      expand_domain<uint16_t>(static_cast<uint16_t*>(domain));
      break;
    case Datatype::UINT32:
      expand_domain<uint32_t>(static_cast<uint32_t*>(domain));
      break;
    case Datatype::UINT64:
      expand_domain<uint64_t>(static_cast<uint64_t*>(domain));
      break;
    default:
      assert(0);
  }
}

template <class T>
void ArraySchema::expand_domain(T* domain) const {
  // Applicable only to regular tiles
  if (tile_extents_ == nullptr)
    return;

  auto tile_extents = static_cast<const T*>(tile_extents_);
  auto array_domain = static_cast<const T*>(domain_);

  for (unsigned int i = 0; i < dim_num_; ++i) {
    domain[2 * i] = ((domain[2 * i] - array_domain[2 * i]) / tile_extents[i] *
                     tile_extents[i]) +
                    array_domain[2 * i];
    domain[2 * i + 1] =
        ((domain[2 * i + 1] - array_domain[2 * i]) / tile_extents[i] + 1) *
            tile_extents[i] -
        1 + array_domain[2 * i];
  }
}

template <class T>
Status ArraySchema::get_cell_pos(const T* coords, uint64_t* pos) const {
  // Applicable only to dense arrays
  if (array_type_ == ArrayType::SPARSE) {
    return LOG_STATUS(Status::ArraySchemaError(
        "Cannot get cell position; Invalid array_schema type"));
  }

  // Invoke the proper function based on the cell order
  if (cell_order_ == Layout::ROW_MAJOR) {
    *pos = get_cell_pos_row(coords);
    return Status::Ok();
  }
  if (cell_order_ == Layout::COL_MAJOR) {
    *pos = get_cell_pos_col(coords);
    return Status::Ok();
  }

  return LOG_STATUS(
      Status::ArraySchemaError("Cannot get cell position; Invalid cell order"));
}

template <class T>
void ArraySchema::get_next_cell_coords(
    const T* domain, T* cell_coords, bool& coords_retrieved) const {
  // Sanity check
  assert(array_type_ == ArrayType::DENSE);

  // Invoke the proper function based on the tile order
  if (cell_order_ == Layout::ROW_MAJOR)
    get_next_cell_coords_row(domain, cell_coords, coords_retrieved);
  else if (cell_order_ == Layout::COL_MAJOR)
    get_next_cell_coords_col(domain, cell_coords, coords_retrieved);
  else  // Sanity check
    assert(0);
}

template <class T>
void ArraySchema::get_next_tile_coords(const T* domain, T* tile_coords) const {
  // Sanity check
  assert(array_type_ == ArrayType::DENSE);

  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR)
    get_next_tile_coords_row(domain, tile_coords);
  else if (tile_order_ == Layout::COL_MAJOR)
    get_next_tile_coords_col(domain, tile_coords);
  else  // Sanity check
    assert(0);
}

template <class T>
void ArraySchema::get_previous_cell_coords(
    const T* domain, T* cell_coords) const {
  // Sanity check
  assert(array_type_ == ArrayType::DENSE);

  // Invoke the proper function based on the tile order
  if (cell_order_ == Layout::ROW_MAJOR)
    get_previous_cell_coords_row(domain, cell_coords);
  else if (cell_order_ == Layout::COL_MAJOR)
    get_previous_cell_coords_col(domain, cell_coords);
  else  // Sanity check
    assert(0);
}

template <class T>
void ArraySchema::get_subarray_tile_domain(
    const T* subarray, T* tile_domain, T* subarray_tile_domain) const {
  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Get tile domain
  T tile_num;  // Per dimension
  for (unsigned int i = 0; i < dim_num_; ++i) {
    tile_num =
        ceil(double(domain[2 * i + 1] - domain[2 * i] + 1) / tile_extents[i]);
    tile_domain[2 * i] = 0;
    tile_domain[2 * i + 1] = tile_num - 1;
  }

  // Calculate subarray in tile domain
  for (unsigned int i = 0; i < dim_num_; ++i) {
    subarray_tile_domain[2 * i] =
        MAX((subarray[2 * i] - domain[2 * i]) / tile_extents[i],
            tile_domain[2 * i]);
    subarray_tile_domain[2 * i + 1] =
        MIN((subarray[2 * i + 1] - domain[2 * i]) / tile_extents[i],
            tile_domain[2 * i + 1]);
  }
}

template <class T>
uint64_t ArraySchema::get_tile_pos(const T* tile_coords) const {
  // Sanity check
  assert(tile_extents_);

  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR)
    return get_tile_pos_row(tile_coords);

  // ROW MAJOR
  return get_tile_pos_col(tile_coords);
}

template <class T>
uint64_t ArraySchema::get_tile_pos(const T* domain, const T* tile_coords) const {
  // Sanity check
  assert(tile_extents_);

  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR)
    return get_tile_pos_row(domain, tile_coords);
   // COL_MAJOR
  return get_tile_pos_col(domain, tile_coords);
}

template <class T>
void ArraySchema::get_tile_subarray(
    const T* tile_coords, T* tile_subarray) const {
  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  for (unsigned int i = 0; i < dim_num_; ++i) {
    tile_subarray[2 * i] = tile_coords[i] * tile_extents[i] + domain[2 * i];
    tile_subarray[2 * i + 1] =
        (tile_coords[i] + 1) * tile_extents[i] - 1 + domain[2 * i];
  }
}

template <class T>
int ArraySchema::tile_cell_order_cmp(
    const T* coords_a, const T* coords_b) const {
  // Check tile order
  int tile_cmp = tile_order_cmp(coords_a, coords_b);
  if (tile_cmp)
    return tile_cmp;

  // Check cell order
  return cell_order_cmp(coords_a, coords_b);
}

template <typename T>
inline uint64_t ArraySchema::tile_id(const T* cell_coords) const {
  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Trivial case
  if (tile_extents == nullptr)
    return 0;

  // Calculate tile coordinates
  // TODO
  //  T* tile_coords = static_cast<T*>(tile_coords_aux_);

  // TODO: This is VERY inefficient. Fix!
  auto tile_coords = new T[dim_num_];

  for (unsigned int i = 0; i < dim_num_; ++i)
    tile_coords[i] = (cell_coords[i] - domain[2 * i]) / tile_extents[i];

  uint64_t tile_id = get_tile_pos(tile_coords);

  // TODO: remove
  delete[] tile_coords;

  // Return
  return tile_id;
}

template <class T>
int ArraySchema::tile_order_cmp(const T* coords_a, const T* coords_b) const {
  // Calculate tile ids
  auto id_a = tile_id(coords_a);
  auto id_b = tile_id(coords_b);

  // Compare ids
  if (id_a < id_b)
    return -1;
  if (id_a > id_b)
    return 1;

  // id_a == id_b
  return 0;
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

void ArraySchema::clear() {
  array_uri_ = URI();
  cell_num_per_tile_ = 0;
  array_type_ = ArrayType::DENSE;
  capacity_ = constants::capacity;
  cell_order_ = Layout::ROW_MAJOR;
  tile_order_ = Layout::ROW_MAJOR;

  for(auto& attr : attributes_)
    delete attr;
  attributes_.clear();
  attribute_num_ = 0;

  for(auto& dim : dimensions_)
    delete dim;
  dimensions_.clear();
  dim_num_ = 0;

  if (tile_extents_ != nullptr) {
    free(tile_extents_);
    tile_extents_ = nullptr;
  }
  if (domain_ != nullptr) {
    free(domain_);
    domain_ = nullptr;
  }
  if (tile_domain_ != nullptr) {
    free(tile_domain_);
    tile_domain_ = nullptr;
  }
  if (tile_coords_aux_ != nullptr) {
    free(tile_coords_aux_);
    tile_coords_aux_ = nullptr;
  }
}

void ArraySchema::compute_cell_num_per_tile() {
  //  Meaningful only for dense arrays
  if (array_type_ == ArrayType::SPARSE)
    return;

  // Invoke the proper templated function
  Datatype type = coords_type();
  switch(type) {
    case Datatype::INT32:
      compute_cell_num_per_tile<int>();
      break;
    case Datatype::INT64:
      compute_cell_num_per_tile<int64_t>();
      break;
    case Datatype::INT8:
      compute_cell_num_per_tile<int8_t>();
      break;
    case Datatype::UINT8:
      compute_cell_num_per_tile<uint8_t>();
      break;
    case Datatype::INT16:
      compute_cell_num_per_tile<int16_t>();
      break;
    case Datatype::UINT16:
      compute_cell_num_per_tile<uint16_t>();
      break;
    case Datatype::UINT32:
      compute_cell_num_per_tile<uint32_t>();
      break;
    case Datatype::UINT64:
      compute_cell_num_per_tile<uint64_t>();
      break;
    default:
      assert(0);
  }
}

template <class T>
void ArraySchema::compute_cell_num_per_tile() {
  auto tile_extents = static_cast<const T*>(tile_extents_);
  cell_num_per_tile_ = 1;

  for (unsigned int i = 0; i < dim_num_; ++i)
    cell_num_per_tile_ *= tile_extents[i];
}

uint64_t ArraySchema::compute_cell_size(unsigned int i) const {
  assert(i <= attribute_num_);

  // For easy reference
  unsigned int cell_val_num = (i < attribute_num_) ? attributes_[i]->cell_val_num() : 0;
  Datatype type = (i < attribute_num_) ? attributes_[i]->type() : coords_type();

  // Variable-sized cell
  if (i < attribute_num_ && cell_val_num == constants::var_num)
    return constants::var_size;

  // Fixed-sized cell
  uint64_t size = 0;

  // Attributes
  if (i < attribute_num_) {
    if (type == Datatype::CHAR)
      size = cell_val_num * sizeof(char);
    else if (type == Datatype::INT32)
      size = cell_val_num * sizeof(int);
    else if (type == Datatype::INT64)
      size = cell_val_num * sizeof(int64_t);
    else if (type == Datatype::FLOAT32)
      size = cell_val_num * sizeof(float);
    else if (type == Datatype::FLOAT64)
      size = cell_val_num * sizeof(double);
    else if (type == Datatype::INT8)
      size = cell_val_num * sizeof(int8_t);
    else if (type == Datatype::UINT8)
      size = cell_val_num * sizeof(uint8_t);
    else if (type == Datatype::INT16)
      size = cell_val_num * sizeof(int16_t);
    else if (type == Datatype::UINT16)
      size = cell_val_num * sizeof(uint16_t);
    else if (type == Datatype::UINT32)
      size = cell_val_num * sizeof(uint32_t);
    else if (type == Datatype::UINT64)
      size = cell_val_num * sizeof(uint64_t);
  } else {  // Coordinates
    if (type == Datatype::INT32)
      size = dim_num_ * sizeof(int);
    else if (type == Datatype::INT64)
      size = dim_num_ * sizeof(int64_t);
    else if (type == Datatype::FLOAT32)
      size = dim_num_ * sizeof(float);
    else if (type == Datatype::FLOAT64)
      size = dim_num_ * sizeof(double);
    else if (type == Datatype::INT8)
      size = dim_num_ * sizeof(int8_t);
    else if (type == Datatype::UINT8)
      size = dim_num_ * sizeof(uint8_t);
    else if (type == Datatype::INT16)
      size = dim_num_ * sizeof(int16_t);
    else if (type == Datatype::UINT16)
      size = dim_num_ * sizeof(uint16_t);
    else if (type == Datatype::UINT32)
      size = dim_num_ * sizeof(uint32_t);
    else if (type == Datatype::UINT64)
      size = dim_num_ * sizeof(uint64_t);
  }

  return size;
}

void ArraySchema::compute_tile_domain() {
  // For easy reference
  Datatype type = coords_type();

  // Invoke the proper templated function
  if (type == Datatype::INT32)
    compute_tile_domain<int>();
  else if (type == Datatype::INT64)
    compute_tile_domain<int64_t>();
  else if (type == Datatype::FLOAT32)
    compute_tile_domain<float>();
  else if (type == Datatype::FLOAT64)
    compute_tile_domain<double>();
  else if (type == Datatype::INT8)
    compute_tile_domain<int8_t>();
  else if (type == Datatype::UINT8)
    compute_tile_domain<uint8_t>();
  else if (type == Datatype::INT16)
    compute_tile_domain<int16_t>();
  else if (type == Datatype::UINT16)
    compute_tile_domain<uint16_t>();
  else if (type == Datatype::UINT32)
    compute_tile_domain<uint32_t>();
  else if (type == Datatype::UINT64)
    compute_tile_domain<uint64_t>();
  else
    assert(0);
}

template <class T>
void ArraySchema::compute_tile_domain() {
  if (tile_extents_ == nullptr)
    return;

  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Allocate space for the tile domain
  assert(tile_domain_ == NULL);
  tile_domain_ = malloc(2 * dim_num_ * sizeof(T));

  // For easy reference
  auto tile_domain = static_cast<T*>(tile_domain_);
  T tile_num;  // Per dimension

  // Calculate tile domain
  for (unsigned int i = 0; i < dim_num_; ++i) {
    tile_num =
        ceil(double(domain[2 * i + 1] - domain[2 * i] + 1) / tile_extents[i]);
    tile_domain[2 * i] = 0;
    tile_domain[2 * i + 1] = tile_num - 1;
  }
}

void ArraySchema::compute_tile_offsets() {
  Datatype type = coords_type();

  // Invoke the proper templated function
  if (type == Datatype::INT32) {
    compute_tile_offsets<int>();
  } else if (type == Datatype::INT64) {
    compute_tile_offsets<int64_t>();
  } else if (type == Datatype::FLOAT32) {
    compute_tile_offsets<float>();
  } else if (type == Datatype::FLOAT64) {
    compute_tile_offsets<double>();
  } else if (type == Datatype::INT8) {
    compute_tile_offsets<int8_t>();
  } else if (type == Datatype::UINT8) {
    compute_tile_offsets<uint8_t>();
  } else if (type == Datatype::INT16) {
    compute_tile_offsets<int16_t>();
  } else if (type == Datatype::UINT16) {
    compute_tile_offsets<uint16_t>();
  } else if (type == Datatype::UINT32) {
    compute_tile_offsets<uint32_t>();
  } else if (type == Datatype::UINT64) {
    compute_tile_offsets<uint64_t>();
  } else {  // The program should never reach this point
    assert(0);
  }
}

template <class T>
void ArraySchema::compute_tile_offsets() {
  // Applicable only to non-NULL space tiles
  if (tile_extents_ == nullptr)
    return;

  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);
  uint64_t tile_num;  // Per dimension

  // Calculate tile offsets for column-major tile order
  tile_offsets_col_.push_back(1);
  if(dim_num_ > 1) {
    for (unsigned int i = 1; i < dim_num_; ++i) {
      tile_num = (domain[2 * (i - 1) + 1] - domain[2 * (i - 1)] + 1) /
                 tile_extents[i - 1];
      tile_offsets_col_.push_back(tile_offsets_col_.back() * tile_num);
    }
  }

  // Calculate tile offsets for row-major tile order
  tile_offsets_row_.push_back(1);
  if(dim_num_ > 1) {
    for (unsigned int i = dim_num_ - 2; ; --i) {
      tile_num = (domain[2 * (i + 1) + 1] - domain[2 * (i + 1)] + 1) /
                 tile_extents[i + 1];
      tile_offsets_row_.push_back(tile_offsets_row_.back() * tile_num);
      if(i == 0)
        break;
    }
  }
  std::reverse(tile_offsets_row_.begin(), tile_offsets_row_.end());
}

template <class T>
uint64_t ArraySchema::get_cell_pos_col(const T* coords) const {
  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Calculate cell offsets
  uint64_t cell_num;  // Per dimension
  std::vector<uint64_t> cell_offsets;
  cell_offsets.push_back(1);
  for (unsigned int i = 1; i < dim_num_; ++i) {
    cell_num = tile_extents[i - 1];
    cell_offsets.push_back(cell_offsets.back() * cell_num);
  }

  // Calculate position
  T coords_norm;  // Normalized coordinates inside the tile
  uint64_t pos = 0;
  for (unsigned int i = 0; i < dim_num_; ++i) {
    coords_norm = (coords[i] - domain[2 * i]);
    coords_norm -= (coords_norm / tile_extents[i]) * tile_extents[i];
    pos += coords_norm * cell_offsets[i];
  }

  // Return
  return pos;
}

template <class T>
uint64_t ArraySchema::get_cell_pos_row(const T* coords) const {
  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Calculate cell offsets
  uint64_t cell_num;  // Per dimension
  std::vector<uint64_t> cell_offsets;
  cell_offsets.push_back(1);
  if(dim_num_ > 1) {
    for (unsigned int i = dim_num_ - 2; ; --i) {
      cell_num = tile_extents[i + 1];
      cell_offsets.push_back(cell_offsets.back() * cell_num);
      if(i == 0)
        break;
    }
  }
  std::reverse(cell_offsets.begin(), cell_offsets.end());

  // Calculate position
  T coords_norm;  // Normalized coordinates inside the tile
  uint64_t pos = 0;
  for (unsigned int i = 0; i < dim_num_; ++i) {
    coords_norm = (coords[i] - domain[2 * i]);
    coords_norm -= (coords_norm / tile_extents[i]) * tile_extents[i];
    pos += coords_norm * cell_offsets[i];
  }

  // Return
  return pos;
}

template <class T>
void ArraySchema::get_next_cell_coords_col(
    const T* domain, T* cell_coords, bool& coords_retrieved) const {
  unsigned int i = 0;
  ++cell_coords[i];

  while (i < dim_num_ - 1 && cell_coords[i] > domain[2 * i + 1]) {
    cell_coords[i] = domain[2 * i];
    ++cell_coords[++i];
  }

  coords_retrieved = !(i == dim_num_ - 1 && cell_coords[i] > domain[2 * i + 1]);
}

template <class T>
void ArraySchema::get_next_cell_coords_row(
    const T* domain, T* cell_coords, bool& coords_retrieved) const {
  unsigned int i = dim_num_ - 1;
  ++cell_coords[i];

  while (i > 0 && cell_coords[i] > domain[2 * i + 1]) {
    cell_coords[i] = domain[2 * i];
    ++cell_coords[--i];
  }

  coords_retrieved = !(i == 0 && cell_coords[i] > domain[2 * i + 1]);
}

template <class T>
void ArraySchema::get_previous_cell_coords_col(
    const T* domain, T* cell_coords) const {
  unsigned int i = 0;
  --cell_coords[i];

  while (i < dim_num_ - 1 && cell_coords[i] < domain[2 * i]) {
    cell_coords[i] = domain[2 * i + 1];
    --cell_coords[++i];
  }
}

template <class T>
void ArraySchema::get_previous_cell_coords_row(
    const T* domain, T* cell_coords) const {
  unsigned int i = dim_num_ - 1;
  --cell_coords[i];

  while (i > 0 && cell_coords[i] < domain[2 * i]) {
    cell_coords[i] = domain[2 * i + 1];
    --cell_coords[--i];
  }
}

template <class T>
void ArraySchema::get_next_tile_coords_col(
    const T* domain, T* tile_coords) const {
  unsigned int i = 0;
  ++tile_coords[i];

  while (i < dim_num_ - 1 && tile_coords[i] > domain[2 * i + 1]) {
    tile_coords[i] = domain[2 * i];
    ++tile_coords[++i];
  }
}

template <class T>
void ArraySchema::get_next_tile_coords_row(
    const T* domain, T* tile_coords) const {
  unsigned int i = dim_num_ - 1;
  ++tile_coords[i];

  while (i > 0 && tile_coords[i] > domain[2 * i + 1]) {
    tile_coords[i] = domain[2 * i];
    ++tile_coords[--i];
  }
}

template <class T>
uint64_t ArraySchema::get_tile_pos_col(const T* tile_coords) const {
  // Calculate position
  uint64_t pos = 0;
  for (unsigned int i = 0; i < dim_num_; ++i)
    pos += tile_coords[i] * tile_offsets_col_[i];

  // Return
  return pos;
}

template <class T>
uint64_t ArraySchema::get_tile_pos_col(
    const T* domain, const T* tile_coords) const {
  // For easy reference
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Calculate tile offsets
  uint64_t tile_num;  // Per dimension
  std::vector<uint64_t> tile_offsets;
  tile_offsets.push_back(1);
  for (unsigned int i = 1; i < dim_num_; ++i) {
    tile_num = (domain[2 * (i - 1) + 1] - domain[2 * (i - 1)] + 1) /
               tile_extents[i - 1];
    tile_offsets.push_back(tile_offsets.back() * tile_num);
  }

  // Calculate position
  uint64_t pos = 0;
  for (unsigned int i = 0; i < dim_num_; ++i)
    pos += tile_coords[i] * tile_offsets[i];

  // Return
  return pos;
}

template <class T>
uint64_t ArraySchema::get_tile_pos_row(const T* tile_coords) const {
  // Calculate position
  uint64_t pos = 0;
  for (unsigned int i = 0; i < dim_num_; ++i)
    pos += tile_coords[i] * tile_offsets_row_[i];

  // Return
  return pos;
}

template <class T>
uint64_t ArraySchema::get_tile_pos_row(
    const T* domain, const T* tile_coords) const {
  // For easy reference
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Calculate tile offsets
  uint64_t tile_num;  // Per dimension
  std::vector<uint64_t> tile_offsets;
  tile_offsets.push_back(1);
  if(dim_num_ > 1) {
    for (unsigned int i = dim_num_ - 2; ; --i) {
      tile_num = (domain[2 * (i + 1) + 1] - domain[2 * (i + 1)] + 1) /
                 tile_extents[i + 1];
      tile_offsets.push_back(tile_offsets.back() * tile_num);
      if(i == 0)
        break;
    }
  }
  std::reverse(tile_offsets.begin(), tile_offsets.end());

  // Calculate position
  uint64_t pos = 0;
  for (unsigned int i = 0; i < dim_num_; ++i)
    pos += tile_coords[i] * tile_offsets[i];

  // Return
  return pos;
}

template <class T>
bool ArraySchema::is_contained_in_tile_slab_col(const T* range) const {
  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);
  uint64_t tile_l, tile_h;

  // Check if range is not contained in a column tile slab
  for (unsigned int i = 1; i < dim_num_; ++i) {
    tile_l = (uint64_t)floor(double(range[2 * i] - domain[2 * i]) / tile_extents[i]);
    tile_h = (uint64_t)floor(double(range[2 * i + 1] - domain[2 * i]) / tile_extents[i]);
    if (tile_l != tile_h)
      return false;
  }

  // Range contained in the column tile slab
  return true;
}

template <class T>
bool ArraySchema::is_contained_in_tile_slab_row(const T* range) const {
  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);
  uint64_t tile_l, tile_h;

  // Check if range is not contained in a row tile slab
  for (unsigned int i = 0; i < dim_num_ - 1; ++i) {
    tile_l = (uint64_t)floor(double(range[2 * i] - domain[2 * i]) / tile_extents[i]);
    tile_h = (uint64_t)floor(double(range[2 * i + 1] - domain[2 * i]) / tile_extents[i]);
    if (tile_l != tile_h)
      return false;
  }

  // Range contained in the row tile slab
  return true;
}

template <class T>
uint64_t ArraySchema::tile_slab_col_cell_num(const T* subarray) const {
  // For easy reference
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Initialize the cell num to be returned to the maximum number of rows
  // in the slab
  uint64_t cell_num =
      MIN(tile_extents[dim_num_ - 1],
          subarray[2 * (dim_num_ - 1) + 1] - subarray[2 * (dim_num_ - 1)] + 1);

  // Calculate the number of cells in the slab
  for (unsigned int i = 0; i < dim_num_ - 1; ++i)
    cell_num *= (subarray[2 * i + 1] - subarray[2 * i] + 1);

  // Return
  return cell_num;
}

template <class T>
uint64_t ArraySchema::tile_slab_row_cell_num(const T* subarray) const {
  // For easy reference
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Initialize the cell num to be returned to the maximum number of rows
  // in the slab
  uint64_t cell_num = MIN(tile_extents[0], subarray[1] - subarray[0] + 1);

  // Calculate the number of cells in the slab
  for (unsigned int i = 1; i < dim_num_; ++i)
    cell_num *= (subarray[2 * i + 1] - subarray[2 * i] + 1);

  // Return
  return cell_num;
}

// Explicit template instantiations

template int ArraySchema::cell_order_cmp<int>(
    const int* coords_a, const int* coords_b) const;
template int ArraySchema::cell_order_cmp<int64_t>(
    const int64_t* coords_a, const int64_t* coords_b) const;
template int ArraySchema::cell_order_cmp<float>(
    const float* coords_a, const float* coords_b) const;
template int ArraySchema::cell_order_cmp<double>(
    const double* coords_a, const double* coords_b) const;
template int ArraySchema::cell_order_cmp<int8_t>(
    const int8_t* coords_a, const int8_t* coords_b) const;
template int ArraySchema::cell_order_cmp<uint8_t>(
    const uint8_t* coords_a, const uint8_t* coords_b) const;
template int ArraySchema::cell_order_cmp<int16_t>(
    const int16_t* coords_a, const int16_t* coords_b) const;
template int ArraySchema::cell_order_cmp<uint16_t>(
    const uint16_t* coords_a, const uint16_t* coords_b) const;
template int ArraySchema::cell_order_cmp<uint32_t>(
    const uint32_t* coords_a, const uint32_t* coords_b) const;
template int ArraySchema::cell_order_cmp<uint64_t>(
    const uint64_t* coords_a, const uint64_t* coords_b) const;

template Status ArraySchema::get_cell_pos<int>(
    const int* coords, uint64_t* pos) const;
template Status ArraySchema::get_cell_pos<int64_t>(
    const int64_t* coords, uint64_t* pos) const;
template Status ArraySchema::get_cell_pos<float>(
    const float* coords, uint64_t* pos) const;
template Status ArraySchema::get_cell_pos<double>(
    const double* coords, uint64_t* pos) const;
template Status ArraySchema::get_cell_pos<int8_t>(
    const int8_t* coords, uint64_t* pos) const;
template Status ArraySchema::get_cell_pos<uint8_t>(
    const uint8_t* coords, uint64_t* pos) const;
template Status ArraySchema::get_cell_pos<int16_t>(
    const int16_t* coords, uint64_t* pos) const;
template Status ArraySchema::get_cell_pos<uint16_t>(
    const uint16_t* coords, uint64_t* pos) const;
template Status ArraySchema::get_cell_pos<uint32_t>(
    const uint32_t* coords, uint64_t* pos) const;
template Status ArraySchema::get_cell_pos<uint64_t>(
    const uint64_t* coords, uint64_t* pos) const;

template void ArraySchema::get_next_cell_coords<int>(
    const int* domain, int* cell_coords, bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<int64_t>(
    const int64_t* domain, int64_t* cell_coords, bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<float>(
    const float* domain, float* cell_coords, bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<double>(
    const double* domain, double* cell_coords, bool& coords_retrieved) const;

template void ArraySchema::get_next_cell_coords<int8_t>(
    const int8_t* domain, int8_t* cell_coords, bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<uint8_t>(
    const uint8_t* domain, uint8_t* cell_coords, bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<int16_t>(
    const int16_t* domain, int16_t* cell_coords, bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<uint16_t>(
    const uint16_t* domain,
    uint16_t* cell_coords,
    bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<uint32_t>(
    const uint32_t* domain,
    uint32_t* cell_coords,
    bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<uint64_t>(
    const uint64_t* domain,
    uint64_t* cell_coords,
    bool& coords_retrieved) const;

template void ArraySchema::get_next_tile_coords<int>(
    const int* domain, int* tile_coords) const;
template void ArraySchema::get_next_tile_coords<int64_t>(
    const int64_t* domain, int64_t* tile_coords) const;
template void ArraySchema::get_next_tile_coords<float>(
    const float* domain, float* tile_coords) const;
template void ArraySchema::get_next_tile_coords<double>(
    const double* domain, double* tile_coords) const;
template void ArraySchema::get_next_tile_coords<int8_t>(
    const int8_t* domain, int8_t* tile_coords) const;
template void ArraySchema::get_next_tile_coords<uint8_t>(
    const uint8_t* domain, uint8_t* tile_coords) const;
template void ArraySchema::get_next_tile_coords<int16_t>(
    const int16_t* domain, int16_t* tile_coords) const;
template void ArraySchema::get_next_tile_coords<uint16_t>(
    const uint16_t* domain, uint16_t* tile_coords) const;
template void ArraySchema::get_next_tile_coords<uint32_t>(
    const uint32_t* domain, uint32_t* tile_coords) const;
template void ArraySchema::get_next_tile_coords<uint64_t>(
    const uint64_t* domain, uint64_t* tile_coords) const;

template void ArraySchema::get_previous_cell_coords<int>(
    const int* domain, int* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<int64_t>(
    const int64_t* domain, int64_t* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<float>(
    const float* domain, float* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<double>(
    const double* domain, double* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<int8_t>(
    const int8_t* domain, int8_t* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<uint8_t>(
    const uint8_t* domain, uint8_t* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<int16_t>(
    const int16_t* domain, int16_t* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<uint16_t>(
    const uint16_t* domain, uint16_t* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<uint32_t>(
    const uint32_t* domain, uint32_t* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<uint64_t>(
    const uint64_t* domain, uint64_t* cell_coords) const;

template void ArraySchema::get_subarray_tile_domain<int>(
    const int* subarray, int* tile_domain, int* subarray_tile_domain) const;
template void ArraySchema::get_subarray_tile_domain<int64_t>(
    const int64_t* subarray,
    int64_t* tile_domain,
    int64_t* subarray_tile_domain) const;
template void ArraySchema::get_subarray_tile_domain<int8_t>(
    const int8_t* subarray,
    int8_t* tile_domain,
    int8_t* subarray_tile_domain) const;
template void ArraySchema::get_subarray_tile_domain<uint8_t>(
    const uint8_t* subarray,
    uint8_t* tile_domain,
    uint8_t* subarray_tile_domain) const;
template void ArraySchema::get_subarray_tile_domain<int16_t>(
    const int16_t* subarray,
    int16_t* tile_domain,
    int16_t* subarray_tile_domain) const;
template void ArraySchema::get_subarray_tile_domain<uint16_t>(
    const uint16_t* subarray,
    uint16_t* tile_domain,
    uint16_t* subarray_tile_domain) const;
template void ArraySchema::get_subarray_tile_domain<uint32_t>(
    const uint32_t* subarray,
    uint32_t* tile_domain,
    uint32_t* subarray_tile_domain) const;
template void ArraySchema::get_subarray_tile_domain<uint64_t>(
    const uint64_t* subarray,
    uint64_t* tile_domain,
    uint64_t* subarray_tile_domain) const;

template uint64_t ArraySchema::get_tile_pos<int>(
    const int* domain, const int* tile_coords) const;
template uint64_t ArraySchema::get_tile_pos<int64_t>(
    const int64_t* domain, const int64_t* tile_coords) const;
template uint64_t ArraySchema::get_tile_pos<float>(
    const float* domain, const float* tile_coords) const;
template uint64_t ArraySchema::get_tile_pos<double>(
    const double* domain, const double* tile_coords) const;
template uint64_t ArraySchema::get_tile_pos<int8_t>(
    const int8_t* domain, const int8_t* tile_coords) const;
template uint64_t ArraySchema::get_tile_pos<uint8_t>(
    const uint8_t* domain, const uint8_t* tile_coords) const;
template uint64_t ArraySchema::get_tile_pos<int16_t>(
    const int16_t* domain, const int16_t* tile_coords) const;
template uint64_t ArraySchema::get_tile_pos<uint16_t>(
    const uint16_t* domain, const uint16_t* tile_coords) const;
template uint64_t ArraySchema::get_tile_pos<uint32_t>(
    const uint32_t* domain, const uint32_t* tile_coords) const;
template uint64_t ArraySchema::get_tile_pos<uint64_t>(
    const uint64_t* domain, const uint64_t* tile_coords) const;

template void ArraySchema::get_tile_subarray<int>(
    const int* tile_coords, int* tile_subarray) const;
template void ArraySchema::get_tile_subarray<int64_t>(
    const int64_t* tile_coords, int64_t* tile_subarray) const;
template void ArraySchema::get_tile_subarray<int8_t>(
    const int8_t* tile_coords, int8_t* tile_subarray) const;
template void ArraySchema::get_tile_subarray<uint8_t>(
    const uint8_t* tile_coords, uint8_t* tile_subarray) const;
template void ArraySchema::get_tile_subarray<int16_t>(
    const int16_t* tile_coords, int16_t* tile_subarray) const;
template void ArraySchema::get_tile_subarray<uint16_t>(
    const uint16_t* tile_coords, uint16_t* tile_subarray) const;
template void ArraySchema::get_tile_subarray<uint32_t>(
    const uint32_t* tile_coords, uint32_t* tile_subarray) const;
template void ArraySchema::get_tile_subarray<uint64_t>(
    const uint64_t* tile_coords, uint64_t* tile_subarray) const;

template bool ArraySchema::is_contained_in_tile_slab_col<int>(
    const int* range) const;
template bool ArraySchema::is_contained_in_tile_slab_col<int64_t>(
    const int64_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_col<float>(
    const float* range) const;
template bool ArraySchema::is_contained_in_tile_slab_col<double>(
    const double* range) const;
template bool ArraySchema::is_contained_in_tile_slab_col<int8_t>(
    const int8_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_col<uint8_t>(
    const uint8_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_col<int16_t>(
    const int16_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_col<uint16_t>(
    const uint16_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_col<uint32_t>(
    const uint32_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_col<uint64_t>(
    const uint64_t* range) const;

template bool ArraySchema::is_contained_in_tile_slab_row<int>(
    const int* range) const;
template bool ArraySchema::is_contained_in_tile_slab_row<int64_t>(
    const int64_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_row<float>(
    const float* range) const;
template bool ArraySchema::is_contained_in_tile_slab_row<double>(
    const double* range) const;
template bool ArraySchema::is_contained_in_tile_slab_row<int8_t>(
    const int8_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_row<uint8_t>(
    const uint8_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_row<int16_t>(
    const int16_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_row<uint16_t>(
    const uint16_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_row<uint32_t>(
    const uint32_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_row<uint64_t>(
    const uint64_t* range) const;

template unsigned int ArraySchema::subarray_overlap<int>(
    const int* subarray_a, const int* subarray_b, int* overlap_subarray) const;
template unsigned int ArraySchema::subarray_overlap<int64_t>(
    const int64_t* subarray_a,
    const int64_t* subarray_b,
    int64_t* overlap_subarray) const;
template unsigned int ArraySchema::subarray_overlap<float>(
    const float* subarray_a,
    const float* subarray_b,
    float* overlap_subarray) const;
template unsigned int ArraySchema::subarray_overlap<double>(
    const double* subarray_a,
    const double* subarray_b,
    double* overlap_subarray) const;
template unsigned int ArraySchema::subarray_overlap<int8_t>(
    const int8_t* subarray_a,
    const int8_t* subarray_b,
    int8_t* overlap_subarray) const;
template unsigned int ArraySchema::subarray_overlap<uint8_t>(
    const uint8_t* subarray_a,
    const uint8_t* subarray_b,
    uint8_t* overlap_subarray) const;
template unsigned int ArraySchema::subarray_overlap<int16_t>(
    const int16_t* subarray_a,
    const int16_t* subarray_b,
    int16_t* overlap_subarray) const;
template unsigned int ArraySchema::subarray_overlap<uint16_t>(
    const uint16_t* subarray_a,
    const uint16_t* subarray_b,
    uint16_t* overlap_subarray) const;
template unsigned int ArraySchema::subarray_overlap<uint32_t>(
    const uint32_t* subarray_a,
    const uint32_t* subarray_b,
    uint32_t* overlap_subarray) const;
template unsigned int ArraySchema::subarray_overlap<uint64_t>(
    const uint64_t* subarray_a,
    const uint64_t* subarray_b,
    uint64_t* overlap_subarray) const;

template int ArraySchema::tile_cell_order_cmp<int>(
    const int* coords_a, const int* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<int64_t>(
    const int64_t* coords_a, const int64_t* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<float>(
    const float* coords_a, const float* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<double>(
    const double* coords_a, const double* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<int8_t>(
    const int8_t* coords_a, const int8_t* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<uint8_t>(
    const uint8_t* coords_a, const uint8_t* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<int16_t>(
    const int16_t* coords_a, const int16_t* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<uint16_t>(
    const uint16_t* coords_a, const uint16_t* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<uint32_t>(
    const uint32_t* coords_a, const uint32_t* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<uint64_t>(
    const uint64_t* coords_a, const uint64_t* coords_b) const;

template uint64_t ArraySchema::tile_id<int>(const int* cell_coords) const;
template uint64_t ArraySchema::tile_id<int64_t>(
    const int64_t* cell_coords) const;
template uint64_t ArraySchema::tile_id<float>(const float* cell_coords) const;
template uint64_t ArraySchema::tile_id<double>(const double* cell_coords) const;
template uint64_t ArraySchema::tile_id<int8_t>(const int8_t* cell_coords) const;
template uint64_t ArraySchema::tile_id<uint8_t>(
    const uint8_t* cell_coords) const;
template uint64_t ArraySchema::tile_id<int16_t>(
    const int16_t* cell_coords) const;
template uint64_t ArraySchema::tile_id<uint16_t>(
    const uint16_t* cell_coords) const;
template uint64_t ArraySchema::tile_id<uint32_t>(
    const uint32_t* cell_coords) const;
template uint64_t ArraySchema::tile_id<uint64_t>(
    const uint64_t* cell_coords) const;

}  // namespace tiledb
