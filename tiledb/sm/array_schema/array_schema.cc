/**
 * @file   array_schema.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/buffer/const_buffer.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/misc/hilbert.h"

#include <cassert>
#include <iostream>
#include <set>
#include <sstream>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArraySchema::ArraySchema()
    : ArraySchema(ArrayType::DENSE) {
}

ArraySchema::ArraySchema(ArrayType array_type)
    : array_type_(array_type) {
  allows_dups_ = false;
  array_uri_ = URI();
  capacity_ = constants::capacity;
  cell_order_ = Layout::ROW_MAJOR;
  domain_ = nullptr;
  tile_order_ = Layout::ROW_MAJOR;
  version_ = constants::format_version;

  // Set up default filter pipelines for coords, offsets, and validity values.
  coords_filters_.add_filter(CompressionFilter(
      constants::coords_compression, constants::coords_compression_level));
  cell_var_offsets_filters_.add_filter(CompressionFilter(
      constants::cell_var_offsets_compression,
      constants::cell_var_offsets_compression_level));
  cell_validity_filters_.add_filter(CompressionFilter(
      constants::cell_validity_compression,
      constants::cell_validity_compression_level));
}

ArraySchema::ArraySchema(const ArraySchema* array_schema) {
  allows_dups_ = array_schema->allows_dups_;
  array_uri_ = array_schema->array_uri_;
  array_type_ = array_schema->array_type_;
  domain_ = nullptr;

  capacity_ = array_schema->capacity_;
  cell_order_ = array_schema->cell_order_;
  cell_var_offsets_filters_ = array_schema->cell_var_offsets_filters_;
  cell_validity_filters_ = array_schema->cell_validity_filters_;
  coords_filters_ = array_schema->coords_filters_;
  tile_order_ = array_schema->tile_order_;
  version_ = array_schema->version_;

  set_domain(array_schema->domain_);

  attribute_map_.clear();
  for (auto attr : array_schema->attributes_)
    add_attribute(attr, false);
}

ArraySchema::~ArraySchema() {
  clear();
}

/* ****************************** */
/*               API              */
/* ****************************** */

bool ArraySchema::allows_dups() const {
  return allows_dups_;
}

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
  auto it = attribute_map_.find(name);
  return it == attribute_map_.end() ? nullptr : it->second;
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
  // Special zipped coordinates attribute
  if (name == constants::coords) {
    auto dim_num = domain_->dim_num();
    assert(dim_num > 0);
    auto coord_size = domain_->dimension(0)->coord_size();
    return dim_num * coord_size;
  }

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

const FilterPipeline& ArraySchema::cell_var_offsets_filters() const {
  return cell_var_offsets_filters_;
}

const FilterPipeline& ArraySchema::cell_validity_filters() const {
  return cell_validity_filters_;
}

Status ArraySchema::check() const {
  if (domain_ == nullptr)
    return LOG_STATUS(
        Status::ArraySchemaError("Array schema check failed; Domain not set"));

  auto dim_num = this->dim_num();
  if (dim_num == 0)
    return LOG_STATUS(Status::ArraySchemaError(
        "Array schema check failed; No dimensions provided"));

  if (cell_order_ == Layout::HILBERT && dim_num > Hilbert::HC_MAX_DIM) {
    return LOG_STATUS(Status::ArraySchemaError(
        "Array schema check failed; Maximum dimensions supported by Hilbert "
        "order exceeded"));
  }

  if (array_type_ == ArrayType::DENSE) {
    auto type = domain_->dimension(0)->type();
    if (datatype_is_real(type)) {
      return LOG_STATUS(
          Status::ArraySchemaError("Array schema check failed; Dense arrays "
                                   "cannot have floating point domains"));
    }
    if (attributes_.size() == 0) {
      return LOG_STATUS(Status::ArraySchemaError(
          "Array schema check failed; No attributes provided"));
    }
  }

  RETURN_NOT_OK(check_double_delta_compressor());

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

const FilterPipeline& ArraySchema::filters(const std::string& name) const {
  if (name == constants::coords)
    return coords_filters();

  // Attribute
  auto attr_it = attribute_map_.find(name);
  if (attr_it != attribute_map_.end())
    return attr_it->second->filters();

  // Dimension (if filters not set, return default coordinate filters)
  auto dim_it = dim_map_.find(name);
  assert(dim_it != dim_map_.end());
  const auto& ret = dim_it->second->filters();
  return !ret.empty() ? ret : coords_filters();
}

const FilterPipeline& ArraySchema::coords_filters() const {
  return coords_filters_;
}

bool ArraySchema::dense() const {
  return array_type_ == ArrayType::DENSE;
}

const Dimension* ArraySchema::dimension(unsigned int i) const {
  return domain_->dimension(i);
}

const Dimension* ArraySchema::dimension(const std::string& name) const {
  auto it = dim_map_.find(name);
  return it == dim_map_.end() ? nullptr : it->second;
}

std::vector<std::string> ArraySchema::dim_names() const {
  auto dim_num = this->dim_num();
  std::vector<std::string> ret;
  ret.reserve(dim_num);
  for (uint32_t d = 0; d < dim_num; ++d)
    ret.emplace_back(domain_->dimension(d)->name());

  return ret;
}

std::vector<Datatype> ArraySchema::dim_types() const {
  auto dim_num = this->dim_num();
  std::vector<Datatype> ret;
  ret.reserve(dim_num);
  for (uint32_t d = 0; d < dim_num; ++d)
    ret.emplace_back(domain_->dimension(d)->type());

  return ret;
}

unsigned int ArraySchema::dim_num() const {
  return domain_->dim_num();
}

void ArraySchema::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;

  std::stringstream ss;
  ss << "- Array type: " << array_type_str(array_type_) << "\n";
  ss << "- Cell order: " << layout_str(cell_order_) << "\n";
  ss << "- Tile order: " << layout_str(tile_order_) << "\n";
  ss << "- Capacity: " << capacity_ << "\n";
  ss << "- Allows duplicates: " << (allows_dups_ ? "true" : "false") << "\n";
  ss << "- Coordinates filters: " << coords_filters_.size();
  fprintf(out, "%s", ss.str().c_str());

  coords_filters_.dump(out);
  fprintf(
      out,
      "\n- Offsets filters: %u",
      (unsigned)cell_var_offsets_filters_.size());
  cell_var_offsets_filters_.dump(out);
  fprintf(
      out, "\n- Validity filters: %u", (unsigned)cell_validity_filters_.size());
  cell_validity_filters_.dump(out);
  fprintf(out, "\n");

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

  for (auto& attr : attributes_) {
    if (name == attr->name()) {
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

bool ArraySchema::is_nullable(const std::string& name) const {
  const Attribute* const attr = this->attribute(name);
  if (attr == nullptr)
    return false;
  return attr->nullable();
}

// ===== FORMAT =====
// version (uint32_t)
// allow_dups (bool)
// array_type (uint8_t)
// tile_order (uint8_t)
// cell_order (uint8_t)
// capacity (uint64_t)
// coords_filters (see FilterPipeline::serialize)
// cell_var_offsets_filters (see FilterPipeline::serialize)
// cell_validity_filters (see FilterPipeline::serialize)
// domain
// attribute_num (uint32_t)
//   attribute #1
//   attribute #2
//   ...
Status ArraySchema::serialize(Buffer* buff) const {
  // Write version, which is always the current version. Despite
  // the in-memory `version_`, we will serialize every array schema
  // as the latest version.
  const uint32_t version = constants::format_version;
  RETURN_NOT_OK(buff->write(&version, sizeof(uint32_t)));

  // Write allows_dups
  RETURN_NOT_OK(buff->write(&allows_dups_, sizeof(bool)));

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

  // Write validity filters
  RETURN_NOT_OK(cell_validity_filters_.serialize(buff));

  // Write domain
  RETURN_NOT_OK(domain_->serialize(buff, version));

  // Write attributes
  auto attribute_num = (uint32_t)attributes_.size();
  RETURN_NOT_OK(buff->write(&attribute_num, sizeof(uint32_t)));
  for (auto& attr : attributes_)
    RETURN_NOT_OK(attr->serialize(buff, version));

  return Status::Ok();
}

Layout ArraySchema::tile_order() const {
  return tile_order_;
}

Datatype ArraySchema::type(const std::string& name) const {
  // Special zipped coordinates attribute
  if (name == constants::coords)
    return domain_->dimension(0)->type();

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
  auto new_attr = tdb_new(Attribute, attr);
  attributes_.emplace_back(new_attr);
  attribute_map_[new_attr->name()] = new_attr;

  return Status::Ok();
}

Status ArraySchema::deserialize(ConstBuffer* buff) {
  // Load version
  RETURN_NOT_OK(buff->read(&version_, sizeof(uint32_t)));

  // Load allows_dups
  if (version_ >= 5)
    RETURN_NOT_OK(buff->read(&allows_dups_, sizeof(bool)));

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

  // Load validity filters
  if (version_ >= 7)
    RETURN_NOT_OK(cell_validity_filters_.deserialize(buff));

  // Load domain
  domain_ = tdb_new(Domain);
  RETURN_NOT_OK(domain_->deserialize(buff, version_));

  // Load attributes
  uint32_t attribute_num;
  RETURN_NOT_OK(buff->read(&attribute_num, sizeof(uint32_t)));
  for (uint32_t i = 0; i < attribute_num; ++i) {
    auto attr = tdb_new(Attribute);
    RETURN_NOT_OK_ELSE(attr->deserialize(buff, version_), tdb_delete(attr));
    attributes_.emplace_back(attr);
    attribute_map_[attr->name()] = attr;
  }

  // Create dimension map
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

  // Success
  return Status::Ok();
}

Status ArraySchema::set_allows_dups(bool allows_dups) {
  if (allows_dups && array_type_ == ArrayType::DENSE)
    return LOG_STATUS(Status::ArraySchemaError(
        "Dense arrays cannot allow coordinate duplicates"));

  allows_dups_ = allows_dups;
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

Status ArraySchema::set_cell_order(Layout cell_order) {
  if (dense() && cell_order == Layout::HILBERT)
    return LOG_STATUS(
        Status::ArraySchemaError("Cannot set cell order; Hilbert order is only "
                                 "applicable to sparse arrays"));

  cell_order_ = cell_order;
  return Status::Ok();
}

Status ArraySchema::set_cell_validity_filter_pipeline(
    const FilterPipeline* pipeline) {
  cell_validity_filters_ = *pipeline;
  return Status::Ok();
}

Status ArraySchema::set_domain(Domain* domain) {
  if (domain == nullptr)
    return LOG_STATUS(
        Status::ArraySchemaError("Cannot set domain; Input domain is nullptr"));

  if (domain->dim_num() == 0)
    return LOG_STATUS(Status::ArraySchemaError(
        "Cannot set domain; Domain must contain at least one dimension"));

  if (array_type_ == ArrayType::DENSE) {
    if (!domain->all_dims_same_type())
      return LOG_STATUS(
          Status::ArraySchemaError("Cannot set domain; In dense arrays, all "
                                   "dimensions must have the same datatype"));

    auto type = domain->dimension(0)->type();
    if (!datatype_is_integer(type) && !datatype_is_datetime(type) &&
        !datatype_is_time(type)) {
      return LOG_STATUS(Status::ArraySchemaError(
          std::string("Cannot set domain; Dense arrays "
                      "do not support dimension datatype '") +
          datatype_str(type) + "'"));
    }
  }

  RETURN_NOT_OK(domain->set_null_tile_extents_to_range());

  // Set domain
  tdb_delete(domain_);
  domain_ = tdb_new(Domain, domain);

  // Create dimension map
  dim_map_.clear();
  auto dim_num = domain_->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = dimension(d);
    dim_map_[dim->name()] = dim;
  }

  return Status::Ok();
}

Status ArraySchema::set_tile_order(Layout tile_order) {
  if (tile_order == Layout::HILBERT)
    return LOG_STATUS(Status::ArraySchemaError(
        "Cannot set tile order; Hilbert order is not applicable to tiles"));

  tile_order_ = tile_order;
  return Status::Ok();
}

void ArraySchema::set_version(uint32_t version) {
  version_ = version;
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

Status ArraySchema::check_double_delta_compressor() const {
  // Check if coordinate filters have DOUBLE DELTA as a compressor
  bool has_double_delta = false;
  for (unsigned i = 0; i < coords_filters_.size(); ++i) {
    if (coords_filters_.get_filter(i)->type() ==
        FilterType::FILTER_DOUBLE_DELTA) {
      has_double_delta = true;
      break;
    }
  }

  // Not applicable when DOUBLE DELTA no present in coord filters
  if (!has_double_delta)
    return Status::Ok();

  // Error if any real dimension inherits the coord filters with DOUBLE DELTA.
  // A dimension inherits the filters when it has no filters.
  auto dim_num = domain_->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = domain_->dimension(d);
    const auto& dim_filters = dim->filters();
    auto dim_type = dim->type();
    if (datatype_is_real(dim_type) && dim_filters.empty())
      return LOG_STATUS(
          Status::ArraySchemaError("Real dimension cannot inherit coordinate "
                                   "filters with DOUBLE DELTA compression"));
  }

  return Status::Ok();
}

void ArraySchema::clear() {
  array_uri_ = URI();
  array_type_ = ArrayType::DENSE;
  capacity_ = constants::capacity;
  cell_order_ = Layout::ROW_MAJOR;
  tile_order_ = Layout::ROW_MAJOR;

  for (auto& attr : attributes_)
    tdb_delete(attr);
  attributes_.clear();

  tdb_delete(domain_);
  domain_ = nullptr;
}

}  // namespace sm
}  // namespace tiledb
