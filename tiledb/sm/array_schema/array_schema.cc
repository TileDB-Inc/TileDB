/**
 * @file   array_schema.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/dimension_label_reference.h"
#include "tiledb/sm/array_schema/dimension_label_schema.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/enums/label_order.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/misc/hilbert.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/storage_format/uri/parse_uri.h"

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
  uri_ = URI();
  name_ = "";
  capacity_ = constants::capacity;
  cell_order_ = Layout::ROW_MAJOR;
  domain_ = nullptr;
  tile_order_ = Layout::ROW_MAJOR;
  version_ = constants::format_version;
  auto timestamp = utils::time::timestamp_now_ms();
  timestamp_range_ = std::make_pair(timestamp, timestamp);

  // Set up default filter pipelines for coords, offsets, and validity values.
  coords_filters_.add_filter(CompressionFilter(
      constants::coords_compression, constants::coords_compression_level));
  cell_var_offsets_filters_.add_filter(CompressionFilter(
      constants::cell_var_offsets_compression,
      constants::cell_var_offsets_compression_level));
  cell_validity_filters_.add_filter(CompressionFilter(
      constants::cell_validity_compression,
      constants::cell_validity_compression_level));

  // Generate URI and name for ArraySchema
  generate_uri();
}

ArraySchema::ArraySchema(
    URI uri,
    uint32_t version,
    std::pair<uint64_t, uint64_t> timestamp_range,
    std::string name,
    ArrayType array_type,
    bool allows_dups,
    shared_ptr<Domain> domain,
    Layout cell_order,
    Layout tile_order,
    uint64_t capacity,
    std::vector<shared_ptr<const Attribute>> attributes,
    std::vector<shared_ptr<const DimensionLabelReference>> dimension_labels,
    FilterPipeline cell_var_offsets_filters,
    FilterPipeline cell_validity_filters,
    FilterPipeline coords_filters)
    : uri_(uri)
    , version_(version)
    , timestamp_range_(timestamp_range)
    , name_(name)
    , array_type_(array_type)
    , allows_dups_(allows_dups)
    , domain_(domain)
    , cell_order_(cell_order)
    , tile_order_(tile_order)
    , capacity_(capacity)
    , attributes_(attributes)
    , dimension_labels_(dimension_labels)
    , cell_var_offsets_filters_(cell_var_offsets_filters)
    , cell_validity_filters_(cell_validity_filters)
    , coords_filters_(coords_filters) {
  Status st;

  // Create dimension map
  dim_map_.clear();
  for (dimension_size_type d = 0; d < domain_->dim_num(); ++d) {
    auto dim{domain_->dimension_ptr(d)};
    dim_map_[dim->name()] = dim;
  }

  // Create attribute map
  if (!attributes_.empty()) {
    for (auto attr_iter : attributes_) {
      auto attr = attr_iter.get();
      attribute_map_[attr->name()] = attr;
    }
  }

  // Create dimension label map
  for (const auto& label : dimension_labels_) {
    dimension_label_map_[label->name()] = label.get();
  }

  // Check array schema is valid.
  st = check_double_delta_compressor(coords_filters_);
  if (!st.ok())
    throw StatusException(
        Status_ArraySchemaError("Array schema check failed; Double delta "
                                "compression used in zipped coords."));

  st = check_string_compressor(coords_filters_);
  if (!st.ok())
    throw StatusException(Status_ArraySchemaError(
        "Array schema check failed; RLE compression used."));

  st = check_attribute_dimension_label_names();
  if (!st.ok())
    throw StatusException(st);
}

ArraySchema::ArraySchema(const ArraySchema& array_schema) {
  allows_dups_ = array_schema.allows_dups_;
  array_uri_ = array_schema.array_uri_;
  uri_ = array_schema.uri_;
  array_type_ = array_schema.array_type_;
  domain_ = nullptr;
  timestamp_range_ = array_schema.timestamp_range_;

  capacity_ = array_schema.capacity_;
  cell_order_ = array_schema.cell_order_;
  cell_var_offsets_filters_ = array_schema.cell_var_offsets_filters_;
  cell_validity_filters_ = array_schema.cell_validity_filters_;
  coords_filters_ = array_schema.coords_filters_;
  tile_order_ = array_schema.tile_order_;
  version_ = array_schema.version_;

  set_domain(array_schema.domain_);

  attribute_map_.clear();
  for (auto attr : array_schema.attributes_)
    add_attribute(attr, false);

  // Create dimension label map
  for (const auto& label : array_schema.dimension_labels_) {
    dimension_labels_.emplace_back(label);
    dimension_label_map_[label->name()] = label.get();
  }

  name_ = array_schema.name_;
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

const Attribute* ArraySchema::attribute(attribute_size_type id) const {
  if (id < attributes_.size())
    return attributes_[id].get();
  return nullptr;
}

const Attribute* ArraySchema::attribute(const std::string& name) const {
  auto it = attribute_map_.find(name);
  return it == attribute_map_.end() ? nullptr : it->second;
}

ArraySchema::attribute_size_type ArraySchema::attribute_num() const {
  return static_cast<attribute_size_type>(attributes_.size());
}

const std::vector<shared_ptr<const Attribute>>& ArraySchema::attributes()
    const {
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
    auto coord_size{domain_->dimension_ptr(0)->coord_size()};
    return dim_num * coord_size;
  }

  if (name == constants::timestamps || name == constants::delete_timestamps) {
    return constants::timestamp_size;
  }

  if (name == constants::delete_condition_index) {
    return sizeof(uint64_t);
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
  // Special attributes
  if (is_special_attribute(name)) {
    return 1;
  }

  // Attribute
  auto attr_it = attribute_map_.find(name);
  if (attr_it != attribute_map_.end()) {
    return attr_it->second->cell_val_num();
  }

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
        Status_ArraySchemaError("Array schema check failed; Domain not set"));

  auto dim_num = this->dim_num();
  if (dim_num == 0)
    return LOG_STATUS(Status_ArraySchemaError(
        "Array schema check failed; No dimensions provided"));

  if (cell_order_ == Layout::HILBERT && dim_num > Hilbert::HC_MAX_DIM) {
    return LOG_STATUS(Status_ArraySchemaError(
        "Array schema check failed; Maximum dimensions supported by Hilbert "
        "order exceeded"));
  }

  if (array_type_ == ArrayType::DENSE) {
    auto type{domain_->dimension_ptr(0)->type()};
    if (datatype_is_real(type)) {
      return LOG_STATUS(
          Status_ArraySchemaError("Array schema check failed; Dense arrays "
                                  "cannot have floating point domains"));
    }
    if (attributes_.size() == 0) {
      return LOG_STATUS(Status_ArraySchemaError(
          "Array schema check failed; No attributes provided"));
    }
  }

  RETURN_NOT_OK(check_double_delta_compressor(coords_filters()));
  RETURN_NOT_OK(check_string_compressor(coords_filters()));
  auto st = check_attribute_dimension_label_names();
  if (!st.ok())
    return LOG_STATUS(st);

  // Check all internal dimension labels have a schema set and the schema is
  // compatible with the definition of the array it was added to.
  //
  // Note: external dimension labels do not need a schema since they are not
  // created when the array is created.
  for (auto label : dimension_labels_) {
    if (!label->is_external()) {
      if (!label->has_schema())
        return LOG_STATUS(Status_ArraySchemaError(
            "Array schema check failed; Missing dimension label schema for "
            "dimension label '" +
            label->name() + "'."));
      if (!label->schema().is_compatible_label(
              domain_->dimension_ptr(label->dimension_id())))
        return LOG_STATUS(Status_ArraySchemaError(
            "Array schema check failed; Dimension label schema for dimension "
            "label '" +
            label->name() + "' is not compatible with the array schema."));
    }
  }

  bool bitsort_filter_exists = false;
  for (const auto &attr : attributes_) {
    // Check that the bitsort filter exists only once within a schema.
    if (attr->filters().has_filter(FilterType::FILTER_BITSORT)) {
      if (bitsort_filter_exists) {
        return LOG_STATUS(Status_ArraySchemaError("Array schema check failed; Bitsort filter cannot exist multiple times in an array schema."));
      }
      bitsort_filter_exists = true;

      if (attr->nullable()) {
        return LOG_STATUS(Status_ArraySchemaError("Bitsort filter does not work for nullable attributes."));
      }
    }
  }

  if (bitsort_filter_exists && array_type_ != ArrayType::SPARSE) {
    return LOG_STATUS(Status_ArraySchemaError("Array schema check failed; Bitsort filter cannot be applied on an array that is not sparse."));
  }

  if (bitsort_filter_exists && !domain_->all_dims_fixed()) {
    return LOG_STATUS(Status_ArraySchemaError("Array schema check failed; Bitsort filter cannot be applied on an array with variable sized dimensions."));
  }

  // Success
  return Status::Ok();
}

Status ArraySchema::check_attributes(
    const std::vector<std::string>& attributes) const {
  for (const auto& attr : attributes) {
    if (attr == constants::coords)
      continue;
    if (attribute_map_.find(attr) == attribute_map_.end())
      return LOG_STATUS(Status_ArraySchemaError(
          "Attribute check failed; cannot find attribute"));
  }

  return Status::Ok();
}

const FilterPipeline& ArraySchema::filters(const std::string& name) const {
  if (is_special_attribute(name)) {
    return coords_filters();
  }

  // Attribute
  auto attr_it = attribute_map_.find(name);
  if (attr_it != attribute_map_.end()) {
    return attr_it->second->filters();
  }

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

const DimensionLabelReference& ArraySchema::dimension_label_reference(
    const dimension_label_size_type i) const {
  return *dimension_labels_[i];
}

const DimensionLabelReference& ArraySchema::dimension_label_reference(
    const std::string& name) const {
  auto iter = dimension_label_map_.find(name);
  if (iter == dimension_label_map_.end())
    throw StatusException(Status_ArraySchemaError(
        "Unable to get dimension label reference; No dimension label named '" +
        name + "'."));
  return *iter->second;
}

const Dimension* ArraySchema::dimension_ptr(dimension_size_type i) const {
  return domain_->dimension_ptr(i);
}

const Dimension* ArraySchema::dimension_ptr(const std::string& name) const {
  auto it = dim_map_.find(name);
  return it == dim_map_.end() ? nullptr : it->second;
}

std::vector<std::string> ArraySchema::dim_names() const {
  auto dim_num = this->dim_num();
  std::vector<std::string> ret;
  ret.reserve(dim_num);
  for (uint32_t d = 0; d < dim_num; ++d)
    ret.emplace_back(domain_->dimension_ptr(d)->name());

  return ret;
}

std::vector<Datatype> ArraySchema::dim_types() const {
  auto dim_num = this->dim_num();
  std::vector<Datatype> ret;
  ret.reserve(dim_num);
  for (uint32_t d = 0; d < dim_num; ++d)
    ret.emplace_back(domain_->dimension_ptr(d)->type());

  return ret;
}

ArraySchema::dimension_label_size_type ArraySchema::dim_label_num() const {
  return static_cast<dimension_label_size_type>(dimension_labels_.size());
}

ArraySchema::dimension_size_type ArraySchema::dim_num() const {
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

  for (auto& label : dimension_labels_) {
    fprintf(out, "\n");
    label->dump(out);
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
  return this->dimension_ptr(name) != nullptr;
}

bool ArraySchema::is_dim_label(const std::string& name) const {
  auto it = dimension_label_map_.find(name);
  return it != dimension_label_map_.end();
}

bool ArraySchema::is_field(const std::string& name) const {
  return is_attr(name) || is_dim(name) || is_special_attribute(name);
}

bool ArraySchema::is_nullable(const std::string& name) const {
  auto attr = this->attribute(name);
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
// dimension_label_num (uint32_t)
//   dimension_label #1
//   dimension_label #2
//   ...
void ArraySchema::serialize(Serializer& serializer) const {
  // Write version, which is always the current version. Despite
  // the in-memory `version_`, we will serialize every array schema
  // as the latest version.
  const uint32_t version = constants::format_version;
  serializer.write<uint32_t>(version);

  // Write allows_dups
  serializer.write<uint8_t>(allows_dups_);

  // Write array type
  auto array_type = (uint8_t)array_type_;
  serializer.write<uint8_t>(array_type);

  // Write tile and cell order
  auto tile_order = (uint8_t)tile_order_;
  serializer.write<uint8_t>(tile_order);
  auto cell_order = (uint8_t)cell_order_;
  serializer.write<uint8_t>(cell_order);

  // Write capacity
  serializer.write<uint64_t>(capacity_);

  // Write coords filters
  coords_filters_.serialize(serializer);

  // Write offsets filters
  cell_var_offsets_filters_.serialize(serializer);

  // Write validity filters
  cell_validity_filters_.serialize(serializer);

  // Write domain
  domain_->serialize(serializer, version);

  // Write attributes
  auto attribute_num = (uint32_t)attributes_.size();
  serializer.write<uint32_t>(attribute_num);
  for (auto& attr : attributes_) {
    attr->serialize(serializer, version);
  }

  // Experimental: Write dimension labels
  if constexpr (is_experimental_build) {
    auto label_num = static_cast<uint32_t>(dimension_labels_.size());
    if (label_num != dimension_labels_.size()) {
      throw StatusException(Status_ArraySchemaError(
          "Overflow when attempting to serialize label number."));
    }
    serializer.write<uint32_t>(label_num);
    for (auto& label : dimension_labels_) {
      label->serialize(serializer, version);
    }
  }
}

Layout ArraySchema::tile_order() const {
  return tile_order_;
}

Datatype ArraySchema::type(const std::string& name) const {
  // Special zipped coordinates attribute
  if (name == constants::coords) {
    return domain_->dimension_ptr(0)->type();
  }

  if (name == constants::timestamps || name == constants::delete_timestamps) {
    return constants::timestamp_type;
  }

  if (name == constants::delete_condition_index) {
    return constants::delete_condition_index_type;
  }

  // Attribute
  auto attr_it = attribute_map_.find(name);
  if (attr_it != attribute_map_.end()) {
    return attr_it->second->type();
  }

  // Dimension
  auto dim_it = dim_map_.find(name);
  assert(dim_it != dim_map_.end());
  return dim_it->second->type();
}

bool ArraySchema::var_size(const std::string& name) const {
  // Special case for zipped coordinates
  if (is_special_attribute(name)) {
    return false;
  }

  // Attribute
  auto attr_it = attribute_map_.find(name);
  if (attr_it != attribute_map_.end()) {
    return attr_it->second->var_size();
  }

  // Dimension
  auto dim_it = dim_map_.find(name);
  if (dim_it != dim_map_.end()) {
    return dim_it->second->var_size();
  }

  // Name is not an attribute or dimension
  assert(false);
  return false;
}

Status ArraySchema::add_attribute(
    shared_ptr<const Attribute> attr, bool check_special) {
  // Sanity check
  if (attr == nullptr)
    return LOG_STATUS(Status_ArraySchemaError(
        "Cannot add attribute; Input attribute is null"));

  // Do not allow attributes with special names
  if (check_special && attr->name().find(constants::special_name_prefix) == 0) {
    std::string msg = "Cannot add attribute; Attribute names starting with '";
    msg += std::string(constants::special_name_prefix) + "' are reserved";
    return LOG_STATUS(Status_ArraySchemaError(msg));
  }

  // Create new attribute and potentially set a default name
  attributes_.emplace_back(attr);
  attribute_map_[attr->name()] = attr.get();

  return Status::Ok();
}

Status ArraySchema::add_dimension_label(
    dimension_size_type dim_id,
    const std::string& name,
    shared_ptr<const DimensionLabelSchema> dimension_label_schema,
    bool check_name,
    bool check_is_compatible) {
  // Check input schema is not null.
  if (dimension_label_schema == nullptr)
    return LOG_STATUS(Status_ArraySchemaError(
        "Cannot add dimension label; Input dimension label schema is null"));
  // Check domain is set and `dim_id` is a valid dimension index.
  if (!domain_)
    return LOG_STATUS(
        Status_ArraySchemaError("Cannot add dimension label; Must set domain "
                                "before adding dimension labels."));
  if (dim_id >= domain_->dim_num())
    return LOG_STATUS(Status_ArraySchemaError(
        "Cannot add a label to dimension " + std::to_string(dim_id) +
        "; Invalid dimension index. "));
  auto dim = domain_->dimension_ptr(dim_id);
  // Check the dimension label is unique among attribute, dimension, and label
  // names; except for possibly matching the name of the dimension it is being
  // added to.
  if (check_name) {
    // Skip attribute/dimension name check if the label name matches the name of
    // the dimension it is added to, since the dimension names also must be
    // unique.
    if (name != dim->name()) {
      // Check no attribute with this name
      bool has_matching_name{false};
      RETURN_NOT_OK(has_attribute(name, &has_matching_name));
      if (has_matching_name)
        return LOG_STATUS(Status_ArraySchemaError(
            "Cannot add a dimension label with name '" + std::string(name) +
            "'. An attribute with that name already exists."));
      // Check no dimension with this name
      RETURN_NOT_OK(domain_->has_dimension(name, &has_matching_name));
      if (has_matching_name)
        return LOG_STATUS(Status_ArraySchemaError(
            "Cannot add a dimension label with name '" + std::string(name) +
            "'. A different dimension with that name already exists."));
    }
    // Check no other dimension label with this name.
    auto found = dimension_label_map_.find(name);
    if (found != dimension_label_map_.end())
      return LOG_STATUS(Status_ArraySchemaError(
          "Cannot add a dimension label with name '" + std::string(name) +
          "'. A different label with that name already exists."));
  }
  // Check the datatype of the dimension label is consistent with the dimension
  // it is being added to.
  if (check_is_compatible && !dimension_label_schema->is_compatible_label(dim))
    return LOG_STATUS(Status_ArraySchemaError(
        "Cannot add dimension label; The dimension label schema is not "
        "compatible with the dimension it is being added to."));
  // Create relative URI in dimension label directory
  URI uri{constants::array_dimension_labels_dir_name + "/l" +
              std::to_string(nlabel_internal_),
          false};
  // Add dimension label
  auto dim_label = make_shared<DimensionLabelReference>(
      HERE(),
      dim_id,
      name,
      uri,
      dimension_label_schema->label_order(),
      dimension_label_schema->label_dimension()->type(),
      dimension_label_schema->label_dimension()->cell_val_num(),
      dimension_label_schema->label_dimension()->domain(),
      dimension_label_schema);
  dimension_labels_.emplace_back(dim_label);
  dimension_label_map_[name] = dim_label.get();
  ++nlabel_internal_;  // WARNING: not atomic
  return Status::Ok();
}

Status ArraySchema::drop_attribute(const std::string& attr_name) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (attr_name.empty()) {
    return LOG_STATUS(
        Status_ArraySchemaError("Cannot remove an empty name attribute"));
  }

  if (attribute_map_.find(attr_name) == attribute_map_.end()) {
    // Not exists.
    return LOG_STATUS(
        Status_ArraySchemaError("Cannot remove a non-exist attribute"));
  }
  attribute_map_.erase(attr_name);

  // Iterate backwards and remove the attribute pointer, it should be slightly
  // faster than iterating forward.
  decltype(attributes_)::iterator it;
  for (it = attributes_.end(); it != attributes_.begin();) {
    --it;
    if ((*it)->name() == attr_name) {
      it = attributes_.erase(it);
    }
  }

  return Status::Ok();
}

// #TODO Add security validation on incoming URI
ArraySchema ArraySchema::deserialize(
    Deserializer& deserializer, const URI& uri) {
  Status st;
  // Load version
  // #TODO Add security validation
  auto version = deserializer.read<uint32_t>();
  if (!(version <= constants::format_version)) {
    throw StatusException(Status_ArraySchemaError(
        "[ArraySchema::deserialize] Incompatible format version."));
  }

  // Load allows_dups
  // Note: No security validation is possible.
  bool allows_dups = false;
  if (version >= 5) {
    allows_dups = deserializer.read<bool>();
  }

  // Load array type
  auto array_type_loaded = deserializer.read<uint8_t>();
  ensure_array_type_is_valid(array_type_loaded);
  ArrayType array_type = ArrayType(array_type_loaded);

  // Load tile order
  auto tile_order_loaded = deserializer.read<uint8_t>();
  try {
    ensure_tile_order_is_valid(tile_order_loaded);
  } catch (std::exception& e) {
    std::throw_with_nested(std::runtime_error("[ArraySchema::deserialize] "));
  }
  Layout tile_order = Layout(tile_order_loaded);

  // Load cell order
  auto cell_order_loaded = deserializer.read<uint8_t>();
  try {
    ensure_cell_order_is_valid(cell_order_loaded);
  } catch (std::exception& e) {
    std::throw_with_nested(std::runtime_error("[ArraySchema::deserialize] "));
  }
  Layout cell_order = Layout(cell_order_loaded);

  // Load capacity
  // #TODO Add security validation
  auto capacity = deserializer.read<uint64_t>();

  // Load filters
  // Note: Security validation delegated to invoked API
  auto coords_filters{FilterPipeline::deserialize(deserializer, version)};
  auto cell_var_filters{FilterPipeline::deserialize(deserializer, version)};
  FilterPipeline cell_validity_filters;
  if (version >= 7) {
    cell_validity_filters = FilterPipeline::deserialize(deserializer, version);
  }

  // Load domain
  // Note: Security validation delegated to invoked API
  // #TODO Add security validation
  auto domain{
      Domain::deserialize(deserializer, version, cell_order, tile_order)};

  // Load attributes
  // Note: Security validation delegated to invoked API
  // #TODO Add security validation
  std::vector<shared_ptr<const Attribute>> attributes;
  uint32_t attribute_num = deserializer.read<uint32_t>();
  for (uint32_t i = 0; i < attribute_num; ++i) {
    auto attr{Attribute::deserialize(deserializer, version)};
    attributes.emplace_back(make_shared<Attribute>(HERE(), move(attr)));
  }

  // Experimental: Load dimension labels
  std::vector<shared_ptr<const DimensionLabelReference>> dimension_labels;
  if constexpr (is_experimental_build) {
    if (version == constants::format_version) {
      uint32_t label_num = deserializer.read<uint32_t>();
      for (uint32_t i{0}; i < label_num; ++i) {
        dimension_labels.emplace_back(
            DimensionLabelReference::deserialize(deserializer, version));
      }
    }
  }

  // Validate
  if (cell_order == Layout::HILBERT &&
      domain->dim_num() > Hilbert::HC_MAX_DIM) {
    throw StatusException(Status_ArraySchemaError(
        "Array schema check failed; Maximum dimensions supported by Hilbert "
        "order exceeded"));
  }

  if (array_type == ArrayType::DENSE) {
    auto type{domain->dimension_ptr(0)->type()};
    if (datatype_is_real(type)) {
      throw StatusException(
          Status_ArraySchemaError("Array schema check failed; Dense arrays "
                                  "cannot have floating point domains"));
    }
    if (attributes.size() == 0) {
      throw StatusException(Status_ArraySchemaError(
          "Array schema check failed; No attributes provided"));
    }
  }

  // Populate timestamp range
  std::pair<uint64_t, uint64_t> timestamp_range;
  throw_if_not_ok(utils::parse::get_timestamp_range(uri, &timestamp_range));

  // Set schema name
  std::string name = uri.last_path_part();

  // Success
  return ArraySchema(
      uri,
      version,
      timestamp_range,
      name,
      array_type,
      allows_dups,
      domain,
      cell_order,
      tile_order,
      capacity,
      attributes,
      dimension_labels,
      cell_var_filters,
      cell_validity_filters,
      coords_filters);
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
    return LOG_STATUS(Status_ArraySchemaError(
        "Dense arrays cannot allow coordinate duplicates"));

  allows_dups_ = allows_dups;
  return Status::Ok();
}

void ArraySchema::set_array_uri(const URI& array_uri) {
  array_uri_ = array_uri;
}

void ArraySchema::set_name(const std::string& name) {
  name_ = name;
}

void ArraySchema::set_capacity(uint64_t capacity) {
  capacity_ = capacity;
}

Status ArraySchema::set_coords_filter_pipeline(const FilterPipeline* pipeline) {
  assert(pipeline);
  RETURN_NOT_OK(check_string_compressor(*pipeline));
  RETURN_NOT_OK(check_double_delta_compressor(*pipeline));

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
        Status_ArraySchemaError("Cannot set cell order; Hilbert order is only "
                                "applicable to sparse arrays"));

  cell_order_ = cell_order;

  return Status::Ok();
}

Status ArraySchema::set_cell_validity_filter_pipeline(
    const FilterPipeline* pipeline) {
  cell_validity_filters_ = *pipeline;
  return Status::Ok();
}

Status ArraySchema::set_domain(shared_ptr<Domain> domain) {
  if (domain == nullptr)
    return LOG_STATUS(
        Status_ArraySchemaError("Cannot set domain; Input domain is nullptr"));

  if (domain->dim_num() == 0)
    return LOG_STATUS(Status_ArraySchemaError(
        "Cannot set domain; Domain must contain at least one dimension"));

  if (array_type_ == ArrayType::DENSE) {
    if (!domain->all_dims_same_type())
      return LOG_STATUS(
          Status_ArraySchemaError("Cannot set domain; In dense arrays, all "
                                  "dimensions must have the same datatype"));

    auto type{domain->dimension_ptr(0)->type()};
    if (!datatype_is_integer(type) && !datatype_is_datetime(type) &&
        !datatype_is_time(type)) {
      return LOG_STATUS(Status_ArraySchemaError(
          std::string("Cannot set domain; Dense arrays "
                      "do not support dimension datatype '") +
          datatype_str(type) + "'"));
    }
  }

  if (cell_order_ != Layout::HILBERT) {
    RETURN_NOT_OK(domain->set_null_tile_extents_to_range());
  }

  // Set domain
  domain_ = domain;

  // Create dimension map
  dim_map_.clear();
  auto dim_num = domain_->dim_num();
  for (dimension_size_type d = 0; d < dim_num; ++d) {
    auto dim{dimension_ptr(d)};
    dim_map_[dim->name()] = dim;
  }

  return Status::Ok();
}

Status ArraySchema::set_tile_order(Layout tile_order) {
  if (tile_order == Layout::HILBERT)
    return LOG_STATUS(Status_ArraySchemaError(
        "Cannot set tile order; Hilbert order is not applicable to tiles"));

  tile_order_ = tile_order;
  return Status::Ok();
}

void ArraySchema::set_version(uint32_t version) {
  version_ = version;
}

uint32_t ArraySchema::write_version() const {
  return version_ < constants::back_compat_writes_min_format_version ?
             constants::format_version :
             version_;
}

uint32_t ArraySchema::version() const {
  return version_;
}

Status ArraySchema::set_timestamp_range(
    const std::pair<uint64_t, uint64_t>& timestamp_range) {
  timestamp_range_ = timestamp_range;
  return Status::Ok();
}

std::pair<uint64_t, uint64_t> ArraySchema::timestamp_range() const {
  return std::pair<uint64_t, uint64_t>(
      timestamp_range_.first, timestamp_range_.second);
}

uint64_t ArraySchema::timestamp_start() const {
  return timestamp_range_.first;
}

const URI& ArraySchema::uri() const {
  return uri_;
}

const std::string& ArraySchema::name() const {
  return name_;
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

Status ArraySchema::check_attribute_dimension_label_names() const {
  std::set<std::string> names;
  // Check attribute and dimension names are unique.
  auto dim_num = this->dim_num();
  uint64_t expected_unique_names{dim_num + attributes_.size()};
  for (auto attr : attributes_)
    names.insert(attr->name());
  for (dimension_size_type i = 0; i < dim_num; ++i)
    names.insert(domain_->dimension_ptr(i)->name());
  if (names.size() != expected_unique_names)
    return Status_ArraySchemaError(
        "Array schema check failed; Attributes and dimensions must have unique "
        "names");
  // Check dimension label names are unique except at most 1 label / dimension
  // that has the same name as the dimension it is on.
  expected_unique_names += dimension_labels_.size();
  std::vector<bool> label_with_dim_name(dim_num, false);
  for (const auto& label : dimension_labels_) {
    const auto& label_name = label->name();
    const auto dim_id = label->dimension_id();
    // Check if the dimension label has the same name as the dimension
    if (label_name == domain_->dimension_ptr(dim_id)->name()) {
      // Check if there is already a dimension label with that name.
      if (label_with_dim_name[dim_id])
        return Status_ArraySchemaError(
            "Array schema check failed; At most one dimension label can share "
            "a name with the dimension it is on");
      --expected_unique_names;  // decrement number of unique name - this name
                                // is not unique
      label_with_dim_name[dim_id] = true;
    } else {
      names.insert(label_name);
    }
  }
  return (names.size() == expected_unique_names) ?
             Status::Ok() :
             Status_ArraySchemaError(
                 "Array schema check failed; Dimension labels must have unique "
                 "names from other labels, attributes, and dimensions");
}

Status ArraySchema::check_double_delta_compressor(
    const FilterPipeline& coords_filters) const {
  // Check if coordinate filters have DOUBLE DELTA as a compressor
  bool has_double_delta = false;
  for (unsigned i = 0; i < coords_filters.size(); ++i) {
    if (coords_filters.get_filter(i)->type() ==
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
  for (dimension_size_type d = 0; d < dim_num; ++d) {
    auto dim{domain_->dimension_ptr(d)};
    const auto& dim_filters = dim->filters();
    auto dim_type = dim->type();
    if (datatype_is_real(dim_type) && dim_filters.empty())
      return LOG_STATUS(
          Status_ArraySchemaError("Real dimension cannot inherit coordinate "
                                  "filters with DOUBLE DELTA compression"));
  }

  return Status::Ok();
}

Status ArraySchema::check_string_compressor(
    const FilterPipeline& filters) const {
  // There is no error if only 1 filter is used for RLE or Dictionary-encoding
  if (filters.size() <= 1 ||
      !(filters.has_filter(FilterType::FILTER_RLE) ||
        filters.has_filter(FilterType::FILTER_DICTIONARY))) {
    return Status::Ok();
  }

  // Error if there are also other filters set for a string dimension together
  // with RLE or Dictionary-encoding
  auto dim_num = domain_->dim_num();
  for (dimension_size_type d = 0; d < dim_num; ++d) {
    auto dim{domain_->dimension_ptr(d)};
    const auto& dim_filters = dim->filters();
    // if it's a var-length string dimension and there is no specific filter
    // list already set for that dimension (then coords_filters_ will be used)
    if (dim->type() == Datatype::STRING_ASCII && dim->var_size() &&
        dim_filters.empty()) {
      if (filters.has_filter(FilterType::FILTER_RLE)) {
        return LOG_STATUS(Status_ArraySchemaError(
            "RLE filter cannot be combined with other filters when applied to "
            "variable length string dimensions"));
      } else {
        return LOG_STATUS(Status_ArraySchemaError(
            "Dictionary-encoding filter cannot be combined with other filters "
            "when applied to variable length string dimensions"));
      }
    }
  }

  return Status::Ok();
}

void ArraySchema::clear() {
  array_uri_ = URI();
  uri_ = URI();
  name_.clear();
  array_type_ = ArrayType::DENSE;
  capacity_ = constants::capacity;
  cell_order_ = Layout::ROW_MAJOR;
  tile_order_ = Layout::ROW_MAJOR;
  attributes_.clear();

  domain_ = nullptr;
  timestamp_range_ = std::make_pair(0, 0);
}

Status ArraySchema::generate_uri() {
  std::string uuid;
  RETURN_NOT_OK(uuid::generate_uuid(&uuid, false));

  auto timestamp = utils::time::timestamp_now_ms();
  timestamp_range_ = std::make_pair(timestamp, timestamp);
  std::stringstream ss;
  ss << "__" << timestamp_range_.first << "_" << timestamp_range_.second << "_"
     << uuid;
  name_ = ss.str();
  uri_ =
      array_uri_.join_path(constants::array_schema_dir_name).join_path(name_);

  return Status::Ok();
}

Status ArraySchema::generate_uri(
    const std::pair<uint64_t, uint64_t>& timestamp_range) {
  std::string uuid;
  RETURN_NOT_OK(uuid::generate_uuid(&uuid, false));

  timestamp_range_ = timestamp_range;
  std::stringstream ss;
  ss << "__" << timestamp_range_.first << "_" << timestamp_range_.second << "_"
     << uuid;
  name_ = ss.str();
  uri_ =
      array_uri_.join_path(constants::array_schema_dir_name).join_path(name_);

  return Status::Ok();
}

std::optional<std::string> ArraySchema::has_bitsort_filter() const {
  for (const auto& attr_it : attribute_map_) {
    if (attr_it.second->filters().has_filter(FilterType::FILTER_BITSORT)) {
      return attr_it.first;
    }
  }

  return std::nullopt;
}

}  // namespace sm
}  // namespace tiledb
