/**
 * @file   array_schema.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
#include "tiledb/sm/buffer/const_buffer.h"
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
  is_kv_ = false;
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
  is_kv_ = false;
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
  is_kv_ = array_schema->is_kv_;
  domain_ = nullptr;

  capacity_ = array_schema->capacity_;
  cell_order_ = array_schema->cell_order_;
  cell_sizes_ = array_schema->cell_sizes_;
  cell_var_offsets_filters_ = array_schema->cell_var_offsets_filters_;
  coords_filters_ = array_schema->coords_filters_;
  coords_size_ = array_schema->coords_size_;
  tile_order_ = array_schema->tile_order_;
  version_ = array_schema->version_;

  if (is_kv_) {
    set_kv_attributes();
    set_kv_domain();
  } else {
    set_domain(array_schema->domain_);
  }

  for (auto attr : array_schema->attributes_) {
    if (attr->name() != constants::key_attr_name)
      add_attribute(attr, false);
  }
  for (const auto& attr : attributes_)
    attribute_map_[attr->name()] = attr;
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

const Attribute* ArraySchema::attribute(std::string name) const {
  bool anonymous = name.empty();
  unsigned int nattr = attribute_num();
  for (unsigned int i = 0; i < nattr; i++) {
    auto attr = attribute(i);
    if ((attr->name() == name) || (anonymous && attr->is_anonymous())) {
      return attr;
    }
  }
  return nullptr;
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

uint64_t ArraySchema::cell_size(const std::string& attribute) const {
  auto cell_size_it = cell_sizes_.find(attribute);
  assert(cell_size_it != cell_sizes_.end());
  return cell_size_it->second;
}

unsigned int ArraySchema::cell_val_num(const std::string& attribute) const {
  auto it = attribute_map_.find(attribute);
  assert(it != attribute_map_.end());
  return it->second->cell_val_num();
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

const FilterPipeline* ArraySchema::filters(const std::string& attribute) const {
  auto it = attribute_map_.find(attribute);
  if (it == attribute_map_.end()) {
    if (attribute == constants::coords)
      return coords_filters();
    assert(false);   // This should never happen
    return nullptr;  // Return something ad hoc
  }

  return it->second->filters();
}

const FilterPipeline* ArraySchema::coords_filters() const {
  return &coords_filters_;
}

Compressor ArraySchema::coords_compression() const {
  auto compressor = coords_filters_.get_filter<CompressionFilter>();
  assert(compressor != nullptr);
  return compressor->compressor();
}

int ArraySchema::coords_compression_level() const {
  auto compressor = coords_filters_.get_filter<CompressionFilter>();
  assert(compressor != nullptr);
  return compressor->compression_level();
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

unsigned int ArraySchema::dim_num() const {
  return domain_->dim_num();
}

void ArraySchema::dump(FILE* out) const {
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

bool ArraySchema::is_kv() const {
  return is_kv_;
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

Datatype ArraySchema::type(unsigned int i) const {
  auto attribute_num = attributes_.size();
  if (i > attribute_num) {
    LOG_ERROR("Cannot retrieve type; Invalid attribute id");
    assert(false);
  }
  if (i < attribute_num)
    return attributes_[i]->type();
  return domain_->type();
}

Datatype ArraySchema::type(const std::string& attribute) const {
  auto it = attribute_map_.find(attribute);
  if (it == attribute_map_.end()) {
    if (attribute == constants::coords)
      return domain_->type();
    assert(false);          // This should never happen
    return Datatype::INT8;  // Return something ad hoc
  }
  return it->second->type();
}

bool ArraySchema::var_size(const std::string& attribute) const {
  auto it = attribute_map_.find(attribute);
  if (it == attribute_map_.end())
    return false;
  return it->second->var_size();
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
Status ArraySchema::deserialize(ConstBuffer* buff, bool is_kv) {
  is_kv_ = is_kv;

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

  attribute_map_.clear();
  for (const auto& attr : attributes_)
    attribute_map_[attr->name()] = attr;

  // Set cell sizes
  for (auto& attr : attributes_)
    cell_sizes_[attr->name()] = compute_cell_size(attr->name());
  cell_sizes_[constants::coords] = compute_cell_size(constants::coords);

  auto dim_num = domain_->dim_num();
  coords_size_ = dim_num * datatype_size(coords_type());

  // Success
  return Status::Ok();
}

Status ArraySchema::set_as_kv() {
  // Do nothing if the array is already set as a key-value store
  if (is_kv_)
    return Status::Ok();

  // Set the array as sparse
  array_type_ = ArrayType::SPARSE;

  // Set layout
  cell_order_ = Layout::ROW_MAJOR;
  tile_order_ = Layout::ROW_MAJOR;

  // Set key-value special domain
  RETURN_NOT_OK(set_kv_domain());

  // Set key-value special attributes
  RETURN_NOT_OK(set_kv_attributes());

  // Set as key-value
  is_kv_ = true;

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
  // Check if the array is a key-value store
  if (is_kv_)
    return LOG_STATUS(Status::ArraySchemaError(
        "Cannot set domain; The array is defined as a key-value store"));

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
  return Status::Ok();
}

void ArraySchema::set_tile_order(Layout tile_order) {
  tile_order_ = tile_order;
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

uint64_t ArraySchema::compute_cell_size(const std::string& attribute) const {
  // Handle coordinates first
  if (attribute == constants::coords) {
    auto dim_num = domain_->dim_num();
    auto type = coords_type();
    return dim_num * datatype_size(type);
  }

  // Handle attributes
  auto attr_it = attribute_map_.find(attribute);
  assert(attr_it != attribute_map_.end());
  auto attr = attr_it->second;

  // For easy reference
  auto cell_val_num = attr->cell_val_num();
  auto type = attr->type();

  // Variable-sized cell
  return (cell_val_num == constants::var_num) ?
             constants::var_size :
             cell_val_num * datatype_size(type);
}

Status ArraySchema::set_kv_attributes() {
  // Add key attribute
  auto key_attr =
      new Attribute(constants::key_attr_name, constants::key_attr_type);
  key_attr->set_compressor(constants::key_attr_compressor);
  attributes_.emplace_back(key_attr);
  return Status::Ok();
}

Status ArraySchema::set_kv_domain() {
  delete domain_;
  domain_ = new Domain(Datatype::UINT64);
  uint64_t dim_domain[] = {0, UINT64_MAX - 1};

  auto dim_1 = new Dimension(constants::key_dim_1, Datatype::UINT64);
  RETURN_NOT_OK_ELSE(dim_1->set_domain(dim_domain), delete dim_1);
  auto dim_2 = new Dimension(constants::key_dim_2, Datatype::UINT64);
  Status st = dim_2->set_domain(dim_domain);
  if (!st.ok()) {
    delete dim_1;
    delete dim_2;
    return st;
  }

  domain_->add_dimension(dim_1);
  domain_->add_dimension(dim_2);

  delete dim_1;
  delete dim_2;

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
