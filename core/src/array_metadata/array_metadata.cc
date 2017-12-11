/**
 * @file   array_metadata.cc
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
 * This file implements the ArrayMetadata class.
 */

#include "array_metadata.h"
#include "const_buffer.h"
#include "logger.h"

#include <cassert>
#include <iostream>
#include <set>
#include <sstream>

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArrayMetadata::ArrayMetadata() {
  attribute_num_ = 0;
  array_uri_ = URI();
  array_type_ = ArrayType::DENSE;
  capacity_ = constants::capacity;
  cell_order_ = Layout::ROW_MAJOR;
  cell_var_offsets_compression_ = constants::cell_var_offsets_compression;
  cell_var_offsets_compression_level_ =
      constants::cell_var_offsets_compression_level;
  coords_compression_ = constants::coords_compression;
  coords_compression_level_ = constants::coords_compression_level;
  domain_ = nullptr;
  tile_order_ = Layout::ROW_MAJOR;
  std::memcpy(version_, constants::version, sizeof(version_));
}

ArrayMetadata::ArrayMetadata(const ArrayMetadata* array_metadata) {
  array_uri_ = array_metadata->array_uri_;
  array_type_ = array_metadata->array_type_;
  attributes_ = array_metadata->attributes_;
  attribute_num_ = array_metadata->attribute_num();
  capacity_ = array_metadata->capacity_;
  cell_order_ = array_metadata->cell_order_;
  cell_sizes_ = array_metadata->cell_sizes_;
  cell_var_offsets_compression_ = array_metadata->cell_var_offsets_compression_;
  cell_var_offsets_compression_level_ =
      array_metadata->cell_var_offsets_compression_level_;
  coords_compression_ = array_metadata->coords_compression_;
  coords_compression_level_ = array_metadata->coords_compression_level_;
  coords_size_ = array_metadata->coords_size_;
  domain_ = array_metadata->domain_;
  tile_order_ = array_metadata->tile_order_;
  std::memcpy(version_, array_metadata->version_, sizeof(version_));
}

ArrayMetadata::ArrayMetadata(const URI& uri) {
  attribute_num_ = 0;
  array_uri_ = uri;
  array_type_ = ArrayType::DENSE;
  capacity_ = constants::capacity;
  cell_order_ = Layout::ROW_MAJOR;
  cell_var_offsets_compression_ = constants::cell_var_offsets_compression;
  cell_var_offsets_compression_level_ =
      constants::cell_var_offsets_compression_level;
  coords_compression_ = constants::coords_compression;
  coords_compression_level_ = constants::coords_compression_level;
  domain_ = nullptr;
  tile_order_ = Layout::ROW_MAJOR;
  std::memcpy(version_, constants::version, sizeof(version_));
}

ArrayMetadata::~ArrayMetadata() {
  clear();
}

/* ****************************** */
/*               API              */
/* ****************************** */

ArrayType ArrayMetadata::array_type() const {
  return array_type_;
}

const URI& ArrayMetadata::array_uri() const {
  return array_uri_;
}

const Attribute* ArrayMetadata::attribute(unsigned int id) const {
  if (id < attributes_.size())
    return attributes_[id];

  return nullptr;
}

const std::string& ArrayMetadata::attribute_name(
    unsigned int attribute_id) const {
  assert(attribute_id <= attribute_num_ + 1);

  // Special case for the search attribute (same as coordinates)
  if (attribute_id == attribute_num_ + 1)
    attribute_id = attribute_num_;

  return attributes_[attribute_id]->name();
}

Status ArrayMetadata::attribute_id(
    const std::string& attribute_name, unsigned int* id) const {
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
      Status::ArrayMetadataError("Attribute not found: " + attribute_name));
}

std::vector<std::string> ArrayMetadata::attribute_names() const {
  std::vector<std::string> names;
  for (auto& attr : attributes_)
    names.emplace_back(attr->name());
  names.emplace_back(constants::coords);
  return names;
}

unsigned int ArrayMetadata::attribute_num() const {
  return attribute_num_;
}

const std::vector<Attribute*>& ArrayMetadata::attributes() const {
  return attributes_;
}

uint64_t ArrayMetadata::capacity() const {
  return capacity_;
}

Layout ArrayMetadata::cell_order() const {
  return cell_order_;
}

uint64_t ArrayMetadata::cell_size(unsigned int attribute_id) const {
  // Special case for the search tile
  if (attribute_id == attribute_num_ + 1)
    attribute_id = attribute_num_;

  return cell_sizes_[attribute_id];
}

unsigned int ArrayMetadata::cell_val_num(unsigned int attribute_id) const {
  return attributes_[attribute_id]->cell_val_num();
}

Compressor ArrayMetadata::cell_var_offsets_compression() const {
  return cell_var_offsets_compression_;
}

int ArrayMetadata::cell_var_offsets_compression_level() const {
  return cell_var_offsets_compression_level_;
}

Status ArrayMetadata::check() const {
  if (array_uri_.is_invalid())
    return LOG_STATUS(Status::ArrayMetadataError(
        "Array metadata check failed; Invalid array URI"));

  if (domain_ == nullptr)
    return LOG_STATUS(Status::ArrayMetadataError(
        "Array metadata check failed; Domain not set"));

  if (array_type_ == ArrayType::DENSE && domain_->null_tile_extents())
    return LOG_STATUS(
        Status::ArrayMetadataError("Array metadata check failed; Dense arrays "
                                   "should not have null tile extents"));

  if (dim_num() == 0)
    return LOG_STATUS(Status::ArrayMetadataError(
        "Array metadata check failed; No dimensions provided"));

  if (array_type_ == ArrayType ::DENSE && attribute_num_ == 0)
    return LOG_STATUS(Status::ArrayMetadataError(
        "Array metadata check failed; No attributes provided"));

  if (!check_double_delta_compressor())
    return LOG_STATUS(Status::ArrayMetadataError(
        "Array metadata check failed; Double delta compression can be used "
        "only with integer values"));

  if (!check_attribute_dimension_names())
    return LOG_STATUS(
        Status::ArrayMetadataError("Array metadata check failed; Attributes "
                                   "and dimensions must have unique names"));

  // Success
  return Status::Ok();
}

Compressor ArrayMetadata::compression(unsigned int attr_id) const {
  assert(attr_id <= attribute_num_ + 1);

  if (attr_id == attribute_num_ + 1)
    return coords_compression_;

  return attributes_[attr_id]->compressor();
}

int ArrayMetadata::compression_level(unsigned int attr_id) const {
  assert(attr_id <= attribute_num_ + 1);

  if (attr_id == attribute_num_ + 1)
    return coords_compression_level_;

  return attributes_[attr_id]->compression_level();
}

Compressor ArrayMetadata::coords_compression() const {
  return coords_compression_;
}

int ArrayMetadata::coords_compression_level() const {
  return coords_compression_level_;
}

uint64_t ArrayMetadata::coords_size() const {
  return coords_size_;
}

Datatype ArrayMetadata::coords_type() const {
  return domain_->type();
}

bool ArrayMetadata::dense() const {
  return array_type_ == ArrayType::DENSE;
}

const Dimension* ArrayMetadata::dimension(unsigned int i) const {
  return domain_->dimension(i);
}

unsigned int ArrayMetadata::dim_num() const {
  return domain_->dim_num();
}

void ArrayMetadata::dump(FILE* out) const {
  const char* array_type_s = array_type_str(array_type_);
  const char* cell_order_s = layout_str(cell_order_);
  const char* tile_order_s = layout_str(tile_order_);

  fprintf(out, "- Array name: %s\n", array_uri_.to_string().c_str());
  fprintf(out, "- Array type: %s\n", array_type_s);
  fprintf(out, "- Cell order: %s\n", cell_order_s);
  fprintf(out, "- Tile order: %s\n", tile_order_s);
  fprintf(out, "- Capacity: %" PRIu64 "\n", capacity_);
  fprintf(
      out,
      "- Coordinates compressor: %s\n",
      compressor_str(coords_compression_));
  fprintf(
      out,
      "- Coordinates compression level: %d\n\n",
      coords_compression_level_);

  if (domain_ != nullptr)
    domain_->dump(out);

  for (auto& attr : attributes_) {
    fprintf(out, "\n");
    attr->dump(out);
  }
}

Status ArrayMetadata::get_attribute_ids(
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

// ===== FORMAT =====
// version (int[3])
// array_type (char)
// tile_order (char)
// cell_order (char)
// capacity (uint64_t)
// coords_compression (char)
// coords_compression_level (int)
// cell_var_offsets_compression (char)
// cell_var_offsets_compression_level (int)
// domain
// attribute_num (unsigned int)
//   attribute #1
//   attribute #2
//   ...
Status ArrayMetadata::serialize(Buffer* buff) const {
  // Write version
  RETURN_NOT_OK(buff->write(constants::version, sizeof(constants::version)));

  // Write array type
  auto array_type = (char)array_type_;
  RETURN_NOT_OK(buff->write(&array_type, sizeof(char)));

  // Write tile and cell order
  auto tile_order = (char)tile_order_;
  RETURN_NOT_OK(buff->write(&tile_order, sizeof(char)));
  auto cell_order = (char)cell_order_;
  RETURN_NOT_OK(buff->write(&cell_order, sizeof(char)));

  // Write capacity
  RETURN_NOT_OK(buff->write(&capacity_, sizeof(uint64_t)));

  // Write coords compression
  auto compressor = static_cast<char>(coords_compression_);
  RETURN_NOT_OK(buff->write(&compressor, sizeof(char)));
  RETURN_NOT_OK(buff->write(&coords_compression_level_, sizeof(int)));

  // Write offsets compression
  auto offset_compressor = static_cast<char>(cell_var_offsets_compression_);
  RETURN_NOT_OK(buff->write(&offset_compressor, sizeof(char)));
  RETURN_NOT_OK(buff->write(&cell_var_offsets_compression_level_, sizeof(int)));

  // Write domain
  domain_->serialize(buff);

  // Write attributes
  RETURN_NOT_OK(buff->write(&attribute_num_, sizeof(unsigned int)));
  for (auto& attr : attributes_)
    RETURN_NOT_OK(attr->serialize(buff));

  return Status::Ok();
}

Layout ArrayMetadata::tile_order() const {
  return tile_order_;
}

Datatype ArrayMetadata::type(unsigned int i) const {
  if (i > attribute_num_) {
    LOG_ERROR("Cannot retrieve type; Invalid attribute id");
    assert(false);
  }

  if (i < attribute_num_)
    return attributes_[i]->type();

  return domain_->type();
}

uint64_t ArrayMetadata::type_size(unsigned int i) const {
  assert(i <= attribute_num_);

  if (i < attribute_num_)
    return datatype_size(attributes_[i]->type());

  return datatype_size(domain_->type());
}

bool ArrayMetadata::var_size(unsigned int attribute_id) const {
  assert(attribute_id <= attribute_num_);

  if (attribute_id < attribute_num_)
    return attributes_[attribute_id]->cell_val_num() == constants::var_num;

  return false;
}

void ArrayMetadata::add_attribute(const Attribute* attr) {
  // Create new attribute and potentially set a default name
  auto new_attr = new Attribute(attr);
  if (new_attr->name().empty())
    new_attr->set_name(constants::default_attr_name);

  attributes_.emplace_back(new_attr);
  ++attribute_num_;
}

// ===== FORMAT =====
// version (int[3])
// array_type (char)
// tile_order (char)
// cell_order (char)
// capacity (uint64_t)
// coords_compression (char)
// coords_compression_level (int)
// cell_var_offsets_compression (char)
// cell_var_offsets_compression_level (int)
// domain
// attribute_num (unsigned int)
//   attribute #1
//   attribute #2
//   ...
Status ArrayMetadata::deserialize(ConstBuffer* buff) {
  // Load version
  RETURN_NOT_OK(buff->read(version_, sizeof(version_)));

  // Load array type
  char array_type;
  RETURN_NOT_OK(buff->read(&array_type, sizeof(char)));
  array_type_ = (ArrayType)array_type;

  // Load tile order
  char tile_order;
  RETURN_NOT_OK(buff->read(&tile_order, sizeof(char)));
  tile_order_ = (Layout)tile_order;

  // Load cell order
  char cell_order;
  RETURN_NOT_OK(buff->read(&cell_order, sizeof(char)));
  cell_order_ = (Layout)cell_order;

  // Load capacity
  RETURN_NOT_OK(buff->read(&capacity_, sizeof(uint64_t)));

  // Load coords compression
  char compressor;
  RETURN_NOT_OK(buff->read(&compressor, sizeof(char)));
  coords_compression_ = static_cast<Compressor>(compressor);
  RETURN_NOT_OK(buff->read(&coords_compression_level_, sizeof(int)));

  // Load offsets compression
  char offsets_compressor;
  RETURN_NOT_OK(buff->read(&offsets_compressor, sizeof(char)));
  cell_var_offsets_compression_ = static_cast<Compressor>(offsets_compressor);
  RETURN_NOT_OK(buff->read(&cell_var_offsets_compression_level_, sizeof(int)));

  // Load domain
  domain_ = new Domain();
  RETURN_NOT_OK(domain_->deserialize(buff));

  // Load attributes
  RETURN_NOT_OK(buff->read(&attribute_num_, sizeof(unsigned int)));
  for (unsigned int i = 0; i < attribute_num_; ++i) {
    auto attr = new Attribute();
    attr->deserialize(buff);
    attributes_.emplace_back(attr);
  }

  // Initialize the rest of the object members
  RETURN_NOT_OK(init());

  // Success
  return Status::Ok();
}

const Domain* ArrayMetadata::domain() const {
  return domain_;
}

Status ArrayMetadata::init() {
  // Perform check of all members
  RETURN_NOT_OK(check());

  // Initialize domain
  RETURN_NOT_OK(domain_->init(cell_order_, tile_order_));

  // Set cell sizes
  cell_sizes_.resize(attribute_num_ + 1);
  for (unsigned int i = 0; i <= attribute_num_; ++i)
    cell_sizes_[i] = compute_cell_size(i);

  auto dim_num = domain_->dim_num();
  coords_size_ = dim_num * datatype_size(coords_type());

  // Success
  return Status::Ok();
}

void ArrayMetadata::set_array_type(ArrayType array_type) {
  array_type_ = array_type;
}

void ArrayMetadata::set_capacity(uint64_t capacity) {
  capacity_ = capacity;
}

void ArrayMetadata::set_coords_compressor(Compressor compressor) {
  coords_compression_ = compressor;
}

void ArrayMetadata::set_coords_compression_level(int compression_level) {
  coords_compression_level_ = compression_level;
}

void ArrayMetadata::set_cell_var_offsets_compressor(Compressor compressor) {
  cell_var_offsets_compression_ = compressor;
}

void ArrayMetadata::set_cell_var_offsets_compression_level(
    int compression_level) {
  cell_var_offsets_compression_level_ = compression_level;
}

void ArrayMetadata::set_cell_order(Layout cell_order) {
  cell_order_ = cell_order;
}

void ArrayMetadata::set_domain(Domain* domain) {
  // Set domain
  delete domain_;
  domain_ = new Domain(domain);

  // Potentially change the default coordinates compressor
  if ((domain_->type() == Datatype::FLOAT32 ||
       domain_->type() == Datatype::FLOAT64) &&
      coords_compression_ == Compressor::DOUBLE_DELTA)
    coords_compression_ = constants::real_coords_compression;
}

void ArrayMetadata::set_tile_order(Layout tile_order) {
  tile_order_ = tile_order;
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

bool ArrayMetadata::check_attribute_dimension_names() const {
  std::set<std::string> names;
  auto dim_num = this->dim_num();

  for (auto attr : attributes_)
    names.insert(attr->name());
  for (unsigned int i = 0; i < dim_num; ++i)
    names.insert(domain_->dimension(i)->name());

  return (names.size() == attribute_num_ + dim_num);
}

bool ArrayMetadata::check_double_delta_compressor() const {
  // Check coordinates
  if ((domain_->type() == Datatype::FLOAT32 ||
       domain_->type() == Datatype::FLOAT64) &&
      coords_compression_ == Compressor::DOUBLE_DELTA)
    return false;

  // Check attributes
  for (auto attr : attributes_) {
    if ((attr->type() == Datatype::FLOAT32 ||
         attr->type() == Datatype::FLOAT64) &&
        attr->compressor() == Compressor::DOUBLE_DELTA)
      return false;
  }

  return true;
}

void ArrayMetadata::clear() {
  array_uri_ = URI();
  array_type_ = ArrayType::DENSE;
  capacity_ = constants::capacity;
  cell_order_ = Layout::ROW_MAJOR;
  tile_order_ = Layout::ROW_MAJOR;

  for (auto& attr : attributes_)
    delete attr;
  attributes_.clear();
  attribute_num_ = 0;

  delete domain_;
  domain_ = nullptr;
}

uint64_t ArrayMetadata::compute_cell_size(unsigned int i) const {
  assert(i <= attribute_num_);

  // For easy reference
  unsigned int cell_val_num =
      (i < attribute_num_) ? attributes_[i]->cell_val_num() : 0;
  Datatype type = (i < attribute_num_) ? attributes_[i]->type() : coords_type();
  auto dim_num = domain_->dim_num();

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
      size = dim_num * sizeof(int);
    else if (type == Datatype::INT64)
      size = dim_num * sizeof(int64_t);
    else if (type == Datatype::FLOAT32)
      size = dim_num * sizeof(float);
    else if (type == Datatype::FLOAT64)
      size = dim_num * sizeof(double);
    else if (type == Datatype::INT8)
      size = dim_num * sizeof(int8_t);
    else if (type == Datatype::UINT8)
      size = dim_num * sizeof(uint8_t);
    else if (type == Datatype::INT16)
      size = dim_num * sizeof(int16_t);
    else if (type == Datatype::UINT16)
      size = dim_num * sizeof(uint16_t);
    else if (type == Datatype::UINT32)
      size = dim_num * sizeof(uint32_t);
    else if (type == Datatype::UINT64)
      size = dim_num * sizeof(uint64_t);
  }
  return size;
}

}  // namespace tiledb
