/**
 * @file   array_schema.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/buffer/const_buffer.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/misc/logger.h"

#include <cassert>
#include <iostream>
#include <set>
#include <sstream>

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArraySchema::ArraySchema() {
  array_uri_ = URI();
  array_type_ = ArrayType::DENSE;
  capacity_ = constants::capacity;
  cell_order_ = Layout::ROW_MAJOR;
  domain_ = nullptr;
  tile_order_ = Layout::ROW_MAJOR;
  version_ = constants::format_version;

  // Set up default filter pipelines for coords and offsets
  coords_filters_.add_filter(CompressionFilter(
      constants::coords_compression, constants::coords_compression_level));
  cell_var_offsets_filters_.add_filter(CompressionFilter(
      constants::cell_var_offsets_compression,
      constants::cell_var_offsets_compression_level));
}

ArraySchema::ArraySchema(ArrayType array_type)
    : array_type_(array_type) {
  array_uri_ = URI();
  capacity_ = constants::capacity;
  cell_order_ = Layout::ROW_MAJOR;
  domain_ = nullptr;
  tile_order_ = Layout::ROW_MAJOR;
  version_ = constants::format_version;

  // Set up default filter pipelines for coords and offsets
  coords_filters_.add_filter(CompressionFilter(
      constants::coords_compression, constants::coords_compression_level));
  cell_var_offsets_filters_.add_filter(CompressionFilter(
      constants::cell_var_offsets_compression,
      constants::cell_var_offsets_compression_level));
}

ArraySchema::ArraySchema(const ArraySchema* array_schema) {
  array_uri_ = array_schema->array_uri_;
  array_type_ = array_schema->array_type_;
  domain_ = nullptr;

  capacity_ = array_schema->capacity_;
  cell_order_ = array_schema->cell_order_;
  cell_var_offsets_filters_ = array_schema->cell_var_offsets_filters_;
  coords_filters_ = array_schema->coords_filters_;
  coords_size_ = array_schema->coords_size_;
  tile_order_ = array_schema->tile_order_;
  version_ = array_schema->version_;

  set_domain(array_schema->domain_);

  attribute_map_.clear();
  for (auto attr : array_schema->attributes_) {
    if (attr->name() != constants::key_attr_name)
      add_attribute(attr, false);
  }
}

ArraySchema::~ArraySchema() {
  clear();
}

/* ****************************** */
/*               API              */
/* ****************************** */

ArrayType ArraySchema::array_type() const {
  return array_type_;
}

const URI& ArraySchema::array_uri() const {
  return array_uri_;
}

const Attribute* ArraySchema::attribute(unsigned int id) const {
  if (id < attributes_.size())
    return attributes_[id];
  return nullptr;
}

const Attribute* ArraySchema::attribute(const std::string& name) const {
  auto it =
      attribute_map_.find(name.empty() ? constants::default_attr_name : name);
  return it == attribute_map_.end() ? nullptr : it->second;
}

Status ArraySchema::attribute_name_normalized(
    const char* attribute, std::string* normalized_name) {
  if (attribute == nullptr)
    return Status::AttributeError("Null attribute name");
  *normalized_name =
      attribute[0] == '\0' ? constants::default_attr_name : attribute;
  return Status::Ok();
}

Status ArraySchema::attribute_names_normalized(
    const char** attributes,
    unsigned num_attributes,
    std::vector<std::string>* normalized_names) {
  normalized_names->clear();

  if (attributes == nullptr || num_attributes == 0)
    return Status::Ok();

  for (unsigned i = 0; i < num_attributes; i++) {
    std::string normalized;
    RETURN_NOT_OK(attribute_name_normalized(attributes[i], &normalized));
    normalized_names->push_back(normalized);
  }

  return Status::Ok();
}

unsigned int ArraySchema::attribute_num() const {
  return (unsigned)attributes_.size();
}

const std::vector<Attribute*>& ArraySchema::attributes() const {
  return attributes_;
}

uint64_t ArraySchema::capacity() const {
  return capacity_;
}

Layout ArraySchema::cell_order() const {
  return cell_order_;
}

uint64_t ArraySchema::cell_size(const std::string& name) const {
  // Special zipped coordinates
  if (name == constants::coords)
    return domain_->dim_num() * datatype_size(coords_type());

  // Attribute
  auto attr_it = attribute_map_.find(name);
  if (attr_it != attribute_map_.end()) {
    auto attr = attr_it->second;
    auto cell_val_num = attr->cell_val_num();
    return (cell_val_num == constants::var_num) ?
               constants::var_size :
               cell_val_num * datatype_size(attr->type());
  }

  // Dimension
  auto dim_it = dim_map_.find(name);
  assert(dim_it != dim_map_.end());
  auto dim = dim_it->second;
  auto cell_val_num = dim->cell_val_num();
  return (cell_val_num == constants::var_num) ?
             constants::var_size :
             cell_val_num * datatype_size(dim->type());
}

unsigned int ArraySchema::cell_val_num(const std::string& name) const {
  // Special zipped coordinates
  if (name == constants::coords)
    return 1;

  // Attribute
  auto attr_it = attribute_map_.find(name);
  if (attr_it != attribute_map_.end())
    return attr_it->second->cell_val_num();

  // Dimension
  auto dim_it = dim_map_.find(name);
  assert(dim_it != dim_map_.end());
  return dim_it->second->cell_val_num();
}

const FilterPipeline* ArraySchema::cell_var_offsets_filters() const {
  return &cell_var_offsets_filters_;
}

Status ArraySchema::check() const {
  if (domain_ == nullptr)
    return LOG_STATUS(
        Status::ArraySchemaError("Array schema check failed; Domain not set"));

  if (dim_num() == 0)
    return LOG_STATUS(Status::ArraySchemaError(
        "Array schema check failed; No dimensions provided"));

  if (array_type_ == ArrayType::DENSE) {
    if (domain_->type() == Datatype::FLOAT32 ||
        domain_->type() == Datatype::FLOAT64) {
      return LOG_STATUS(
          Status::ArraySchemaError("Array schema check failed; Dense arrays "
                                   "cannot have floating point domains"));
    }
    if (attributes_.size() == 0) {
      return LOG_STATUS(Status::ArraySchemaError(
          "Array schema check failed; No attributes provided"));
    }
  }

  if (!check_double_delta_compressor())
    return LOG_STATUS(Status::ArraySchemaError(
        "Array schema check failed; Double delta compression can be used "
        "only with integer values"));

  if (!check_attribute_dimension_names())
    return LOG_STATUS(
        Status::ArraySchemaError("Array schema check failed; Attributes "
                                 "and dimensions must have unique names"));

  // Success
  return Status::Ok();
}

Status ArraySchema::check_attributes(
    const std::vector<std::string>& attributes) const {
  for (const auto& attr : attributes) {
    if (attr == constants::coords)
      continue;
    if (attribute_map_.find(attr) == attribute_map_.end())
      return LOG_STATUS(Status::ArraySchemaError(
          "Attribute check failed; cannot find attribute"));
  }

  return Status::Ok();
}

const FilterPipeline* ArraySchema::filters(const std::string& name) const {
  if (name == constants::coords)
    return coords_filters();

  // Attribute
  auto attr_it = attribute_map_.find(name);
  if (attr_it != attribute_map_.end())
    return attr_it->second->filters();

  // Dimension (if filters not set, return default coordinate filters)
  auto dim_it = dim_map_.find(name);
  assert(dim_it != dim_map_.end());
  auto ret = dim_it->second->filters();
  return (ret != nullptr) ? ret : coords_filters();
}

const FilterPipeline* ArraySchema::coords_filters() const {
  return &coords_filters_;
}

Compressor ArraySchema::coords_compression() const {
  auto compressor = coords_filters_.get_filter<CompressionFilter>();
  return (compressor == nullptr) ? Compressor::NO_COMPRESSION :
                                   compressor->compressor();
}

int ArraySchema::coords_compression_level() const {
  auto compressor = coords_filters_.get_filter<CompressionFilter>();
  return (compressor == nullptr) ? -1 : compressor->compression_level();
}

uint64_t ArraySchema::coords_size() const {
  return coords_size_;
}

Datatype ArraySchema::coords_type() const {
  return domain_->type();
}

bool ArraySchema::dense() const {
  return array_type_ == ArrayType::DENSE;
}

const Dimension* ArraySchema::dimension(unsigned int i) const {
  return domain_->dimension(i);
}

const Dimension* ArraySchema::dimension(const std::string& name) const {
  auto it = dim_map_.find(name.empty() ? constants::default_dim_name : name);
  return it == dim_map_.end() ? nullptr : it->second;
}

unsigned int ArraySchema::dim_num() const {
  return domain_->dim_num();
}

void ArraySchema::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;
  fprintf(out, "- Array type: %s\n", array_type_str(array_type_).c_str());
  fprintf(out, "- Cell order: %s\n", layout_str(cell_order_).c_str());
  fprintf(out, "- Tile order: %s\n", layout_str(tile_order_).c_str());
  fprintf(out, "- Capacity: %" PRIu64 "\n", capacity_);
  fprintf(
      out,
      "- Coordinates compressor: %s\n",
      compressor_str(coords_compression()).c_str());
  fprintf(
      out,
      "- Coordinates compression level: %d\n\n",
      coords_compression_level());

  if (domain_ != nullptr)
    domain_->dump(out);

  for (auto& attr : attributes_) {
    fprintf(out, "\n");
    attr->dump(out);
  }
}

Status ArraySchema::has_attribute(
    const std::string& name, bool* has_attr) const {
  *has_attr = false;

  std::string normalized;
  RETURN_NOT_OK(attribute_name_normalized(name.c_str(), &normalized));

  for (auto& attr : attributes_) {
    if (normalized == attr->name()) {
      *has_attr = true;
      break;
    }
  }

  return Status::Ok();
}

bool ArraySchema::is_attr(const std::string& name) const {
  return this->attribute(name) != nullptr;
}

bool ArraySchema::is_dim(const std::string& name) const {
  return this->dimension(name) != nullptr;
}

// ===== FORMAT =====
// version (uint32_t)
// array_type (uint8_t)
// tile_order (uint8_t)
// cell_order (uint8_t)
// capacity (uint64_t)
// coords_filters (see FilterPipeline::serialize)
// cell_var_offsets_filters (see FilterPipeline::serialize)
// domain
// attribute_num (uint32_t)
//   attribute #1
//   attribute #2
//   ...
Status ArraySchema::serialize(Buffer* buff) const {
  // Write version
  RETURN_NOT_OK(buff->write(&version_, sizeof(uint32_t)));

  // Write array type
  auto array_type = (uint8_t)array_type_;
  RETURN_NOT_OK(buff->write(&array_type, sizeof(uint8_t)));

  // Write tile and cell order
  auto tile_order = (uint8_t)tile_order_;
  RETURN_NOT_OK(buff->write(&tile_order, sizeof(uint8_t)));
  auto cell_order = (uint8_t)cell_order_;
  RETURN_NOT_OK(buff->write(&cell_order, sizeof(uint8_t)));

  // Write capacity
  RETURN_NOT_OK(buff->write(&capacity_, sizeof(uint64_t)));

  // Write coords filters
  RETURN_NOT_OK(coords_filters_.serialize(buff));

  // Write offsets filters
  RETURN_NOT_OK(cell_var_offsets_filters_.serialize(buff));

  // Write domain
  domain_->serialize(buff);

  // Write attributes
  auto attribute_num = (uint32_t)attributes_.size();
  RETURN_NOT_OK(buff->write(&attribute_num, sizeof(uint32_t)));
  for (auto& attr : attributes_)
    RETURN_NOT_OK(attr->serialize(buff));

  return Status::Ok();
}

Layout ArraySchema::tile_order() const {
  return tile_order_;
}

Datatype ArraySchema::type(const std::string& name) const {
  // Special zipped coordinates
  if (name == constants::coords)
    return domain_->type();

  // Attribute
  auto attr_it = attribute_map_.find(name);
  if (attr_it != attribute_map_.end())
    return attr_it->second->type();

  // Dimension
  auto dim_it = dim_map_.find(name);
  assert(dim_it != dim_map_.end());
  return dim_it->second->type();
}

bool ArraySchema::var_size(const std::string& name) const {
  // Special case for zipped coordinates
  if (name == constants::coords)
    return false;

  // Attribute
  auto attr_it = attribute_map_.find(name);
  if (attr_it != attribute_map_.end())
    return attr_it->second->var_size();

  // Dimension
  auto dim_it = dim_map_.find(name);
  if (dim_it != dim_map_.end())
    return dim_it->second->var_size();

  // Name is not an attribute or dimension
  assert(false);
  return false;
}

Status ArraySchema::add_attribute(const Attribute* attr, bool check_special) {
  // Sanity check
  if (attr == nullptr)
    return LOG_STATUS(Status::ArraySchemaError(
        "Cannot add attribute; Input attribute is null"));

  // Do not allow attributes with special names
  if (check_special && attr->name().find(constants::special_name_prefix) == 0) {
    std::string msg = "Cannot add attribute; Attribute names starting with '";
    msg += std::string(constants::special_name_prefix) + "' are reserved";
    return LOG_STATUS(Status::ArraySchemaError(msg));
  }

  // Create new attribute and potentially set a default name
  auto new_attr = (Attribute*)nullptr;
  if (attr->is_anonymous()) {
    // Check if any other attributes are anonymous
    for (auto& a : attributes_) {
      if (a->is_anonymous()) {
        return LOG_STATUS(Status::ArraySchemaError(
            "Only one anonymous attribute is allowed per array"));
      }
    }
    new_attr = new Attribute(attr);
    new_attr->set_name(constants::default_attr_name);
  } else {
    new_attr = new Attribute(attr);
  }

  attributes_.emplace_back(new_attr);
  attribute_map_[new_attr->name()] = new_attr;

  return Status::Ok();
}

// ===== FORMAT =====
// version (uint32_t)
// array_type (uint8_t)
// tile_order (uint8_t)
// cell_order (uint8_t)
// capacity (uint64_t)
// coords_filters (see FilterPipeline::serialize)
// cell_var_offsets_filters (see FilterPipeline::serialize)
// domain
// attribute_num (uint32_t)
//   attribute #1
//   attribute #2
//   ...
Status ArraySchema::deserialize(ConstBuffer* buff) {
  // Load version
  RETURN_NOT_OK(buff->read(&version_, sizeof(uint32_t)));

  // Load array type
  uint8_t array_type;
  RETURN_NOT_OK(buff->read(&array_type, sizeof(uint8_t)));
  array_type_ = (ArrayType)array_type;

  // Load tile order
  uint8_t tile_order;
  RETURN_NOT_OK(buff->read(&tile_order, sizeof(uint8_t)));
  tile_order_ = (Layout)tile_order;

  // Load cell order
  uint8_t cell_order;
  RETURN_NOT_OK(buff->read(&cell_order, sizeof(uint8_t)));
  cell_order_ = (Layout)cell_order;

  // Load capacity
  RETURN_NOT_OK(buff->read(&capacity_, sizeof(uint64_t)));

  // Load coords filters
  RETURN_NOT_OK(coords_filters_.deserialize(buff));

  // Load offsets filters
  RETURN_NOT_OK(cell_var_offsets_filters_.deserialize(buff));

  // Load domain
  domain_ = new Domain();
  RETURN_NOT_OK(domain_->deserialize(buff));

  // Load attributes
  uint32_t attribute_num;
  RETURN_NOT_OK(buff->read(&attribute_num, sizeof(uint32_t)));
  for (uint32_t i = 0; i < attribute_num; ++i) {
    auto attr = new Attribute();
    RETURN_NOT_OK_ELSE(attr->deserialize(buff), delete attr);
    attributes_.emplace_back(attr);
    attribute_map_[attr->name()] = attr;
  }
  auto dim_num = domain()->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = dimension(d);
    dim_map_[dim->name()] = dim;
  }

  // Initialize the rest of the object members
  RETURN_NOT_OK(init());

  // Success
  return Status::Ok();
}

const Domain* ArraySchema::domain() const {
  return domain_;
}

Status ArraySchema::init() {
  // Perform check of all members
  RETURN_NOT_OK(check());

  // Initialize domain
  RETURN_NOT_OK(domain_->init(cell_order_, tile_order_));

  // Set cell sizes
  // TODO: set upon setting domain
  coords_size_ = domain_->dim_num() * datatype_size(coords_type());

  // Success
  return Status::Ok();
}

void ArraySchema::set_array_uri(const URI& array_uri) {
  array_uri_ = array_uri;
}

void ArraySchema::set_capacity(uint64_t capacity) {
  capacity_ = capacity;
}

Status ArraySchema::set_coords_filter_pipeline(const FilterPipeline* pipeline) {
  coords_filters_ = *pipeline;
  return Status::Ok();
}

Status ArraySchema::set_cell_var_offsets_filter_pipeline(
    const FilterPipeline* pipeline) {
  cell_var_offsets_filters_ = *pipeline;
  return Status::Ok();
}

void ArraySchema::set_cell_order(Layout cell_order) {
  cell_order_ = cell_order;
}

Status ArraySchema::set_domain(Domain* domain) {
  if (array_type_ == ArrayType::DENSE) {
    RETURN_NOT_OK(domain->set_null_tile_extents_to_range());

    if (domain->type() == Datatype::FLOAT32 ||
        domain->type() == Datatype::FLOAT64) {
      return LOG_STATUS(
          Status::ArraySchemaError("Cannot set domain; Dense arrays "
                                   "cannot have floating point domains"));
    }
  }

  // Set domain
  delete domain_;
  domain_ = new Domain(domain);

  // Potentially change the default coordinates compressor
  if ((domain_->type() == Datatype::FLOAT32 ||
       domain_->type() == Datatype::FLOAT64) &&
      coords_compression() == Compressor::DOUBLE_DELTA) {
    auto* filter = coords_filters_.get_filter<CompressionFilter>();
    assert(filter != nullptr);
    filter->set_compressor(constants::real_coords_compression);
    filter->set_compression_level(-1);
  }

  // Create dimension map
  dim_map_.clear();
  auto dim_num = domain_->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = dimension(d);
    dim_map_[dim->name()] = dim;
  }

  return Status::Ok();
}

void ArraySchema::set_tile_order(Layout tile_order) {
  tile_order_ = tile_order;
}

uint32_t ArraySchema::version() const {
  return version_;
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

bool ArraySchema::check_attribute_dimension_names() const {
  std::set<std::string> names;
  auto dim_num = this->dim_num();
  for (auto attr : attributes_)
    names.insert(attr->name());
  for (unsigned int i = 0; i < dim_num; ++i)
    names.insert(domain_->dimension(i)->name());
  return (names.size() == attributes_.size() + dim_num);
}

bool ArraySchema::check_double_delta_compressor() const {
  // Check coordinates
  if ((domain_->type() == Datatype::FLOAT32 ||
       domain_->type() == Datatype::FLOAT64) &&
      coords_compression() == Compressor::DOUBLE_DELTA)
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

void ArraySchema::clear() {
  array_uri_ = URI();
  array_type_ = ArrayType::DENSE;
  capacity_ = constants::capacity;
  cell_order_ = Layout::ROW_MAJOR;
  tile_order_ = Layout::ROW_MAJOR;

  for (auto& attr : attributes_)
    delete attr;
  attributes_.clear();

  delete domain_;
  domain_ = nullptr;
}

}  // namespace sm
}  // namespace tiledb
