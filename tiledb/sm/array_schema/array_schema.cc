/**
 * @file   array_schema.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
#include "tiledb/common/assert.h"
#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/current_domain.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/dimension_label.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/array_schema/enumeration.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/data_order.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/webp_filter.h"
#include "tiledb/sm/fragment/fragment_identifier.h"
#include "tiledb/sm/misc/hilbert.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/storage_format/uri/generate_uri.h"
#include "tiledb/type/apply_with_type.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <set>
#include <sstream>

using namespace tiledb::common;

namespace tiledb::sm {

/** Class for locally generated status exceptions. */
class ArraySchemaException : public StatusException {
 public:
  explicit ArraySchemaException(const std::string& msg)
      : StatusException("ArraySchema", msg) {
  }
};

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArraySchema::ArraySchema(
    ArrayType array_type,
    shared_ptr<MemoryTracker> memory_tracker,
    const std::optional<std::pair<uint64_t, uint64_t>>& timestamp_range)
    : memory_tracker_(memory_tracker)
    , uri_(URI())
    , array_uri_(URI())
    , version_(constants::format_version)
    , name_("")
    , array_type_(array_type)
    , allows_dups_(false)
    , domain_(nullptr)
    , dim_map_(memory_tracker_->get_resource(MemoryType::DIMENSIONS))
    , cell_order_(Layout::ROW_MAJOR)
    , tile_order_(Layout::ROW_MAJOR)
    , capacity_(constants::capacity)
    , attributes_(memory_tracker_->get_resource(MemoryType::ATTRIBUTES))
    , attribute_map_(memory_tracker_->get_resource(MemoryType::ATTRIBUTES))
    , dimension_labels_(
          memory_tracker_->get_resource(MemoryType::DIMENSION_LABELS))
    , dimension_label_map_(
          memory_tracker_->get_resource(MemoryType::DIMENSION_LABELS))
    , enumeration_map_(memory_tracker_->get_resource(MemoryType::ENUMERATION))
    , enumeration_path_map_(
          memory_tracker_->get_resource(MemoryType::ENUMERATION_PATHS))
    , current_domain_(make_shared<CurrentDomain>(
          memory_tracker, constants::current_domain_version)) {
  // Set timestamp to the user passed-in range, otherwise set to the current
  // time
  uint64_t now = utils::time::timestamp_now_ms();
  timestamp_range_ = timestamp_range.value_or(std::make_pair(now, now));

  // Set up default filter pipelines for coords, offsets, and validity values.
  coords_filters_.add_filter(CompressionFilter(
      constants::coords_compression,
      constants::coords_compression_level,
      Datatype::ANY));
  cell_var_offsets_filters_.add_filter(CompressionFilter(
      constants::cell_var_offsets_compression,
      constants::cell_var_offsets_compression_level,
      Datatype::UINT64));
  cell_validity_filters_.add_filter(CompressionFilter(
      constants::cell_validity_compression,
      constants::cell_validity_compression_level,
      Datatype::UINT8));

  // Generate URI and name for ArraySchema
  generate_uri(timestamp_range);
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
    std::vector<shared_ptr<const DimensionLabel>> dim_label_refs,
    std::vector<shared_ptr<const Enumeration>> enumerations,
    std::unordered_map<std::string, std::string> enumeration_path_map,
    FilterPipeline cell_var_offsets_filters,
    FilterPipeline cell_validity_filters,
    FilterPipeline coords_filters,
    shared_ptr<CurrentDomain> current_domain,
    shared_ptr<MemoryTracker> memory_tracker)
    : memory_tracker_(memory_tracker)
    , uri_(uri)
    , version_(version)
    , timestamp_range_(timestamp_range)
    , name_(name)
    , array_type_(array_type)
    , allows_dups_(allows_dups)
    , domain_(domain)
    , dim_map_(memory_tracker_->get_resource(MemoryType::DIMENSIONS))
    , cell_order_(cell_order)
    , tile_order_(tile_order)
    , capacity_(capacity)
    , attributes_(memory_tracker_->get_resource(MemoryType::ATTRIBUTES))
    , attribute_map_(memory_tracker_->get_resource(MemoryType::ATTRIBUTES))
    , dimension_labels_(
          memory_tracker_->get_resource(MemoryType::DIMENSION_LABELS))
    , dimension_label_map_(
          memory_tracker_->get_resource(MemoryType::DIMENSION_LABELS))
    , enumeration_map_(memory_tracker_->get_resource(MemoryType::ENUMERATION))
    , enumeration_path_map_(
          memory_tracker_->get_resource(MemoryType::ENUMERATION_PATHS))
    , cell_var_offsets_filters_(cell_var_offsets_filters)
    , cell_validity_filters_(cell_validity_filters)
    , coords_filters_(coords_filters)
    , current_domain_(current_domain) {
  for (auto atr : attributes) {
    attributes_.push_back(atr);
  }

  for (auto dim_label : dim_label_refs) {
    dimension_labels_.push_back(dim_label);
  }

  for (auto& elem : enumeration_path_map) {
    enumeration_path_map_.insert(elem);
  }

  // Create dimension map
  for (dimension_size_type d = 0; d < domain_->dim_num(); ++d) {
    auto dim{domain_->dimension_ptr(d)};
    dim_map_[dim->name()] = dim;
  }

  for (auto& [enmr_name, enmr_filename] : enumeration_path_map_) {
    (void)enmr_filename;
    enumeration_map_[enmr_name] = nullptr;
  }

  for (const auto& enmr : enumerations) {
    enumeration_map_[enmr->name()] = enmr;
  }

  // Create attribute map
  auto n{attribute_num()};
  for (decltype(n) i = 0; i < n; ++i) {
    auto attr = attributes_[i].get();
    attribute_map_[attr->name()] = {attr, i};
  }

  // Create dimension label map
  for (const auto& label : dimension_labels_) {
    dimension_label_map_[label->name()] = label.get();
  }

  // Check array schema is valid.
  try {
    check_double_delta_compressor(coords_filters_);
  } catch (const StatusException&) {
    throw ArraySchemaException(
        "Array schema check failed; Double delta compression used in zipped "
        "coords.");
  }

  try {
    check_string_compressor(coords_filters_);
  } catch (const StatusException&) {
    throw ArraySchemaException(
        "Array schema check failed; RLE compression used.");
  }

  check_attribute_dimension_label_names();
}

ArraySchema::ArraySchema(const ArraySchema& array_schema)
    : memory_tracker_{array_schema.memory_tracker_}
    , uri_{array_schema.uri_}
    , array_uri_{array_schema.array_uri_}
    , version_{array_schema.version_}
    , timestamp_range_{array_schema.timestamp_range_}
    , name_{array_schema.name_}
    , array_type_{array_schema.array_type_}
    , allows_dups_{array_schema.allows_dups_}
    , domain_{}  // copied below by `set_domain`
    , dim_map_(memory_tracker_->get_resource(
          MemoryType::DIMENSIONS))  // initialized in `set_domain`
    , cell_order_{array_schema.cell_order_}
    , tile_order_{array_schema.tile_order_}
    , capacity_{array_schema.capacity_}
    , attributes_(
          array_schema.attributes_,
          memory_tracker_->get_resource(MemoryType::ATTRIBUTES))
    , attribute_map_(
          array_schema.attribute_map_,
          memory_tracker_->get_resource(MemoryType::ATTRIBUTES))
    , dimension_labels_(memory_tracker_->get_resource(
          MemoryType::DIMENSION_LABELS))  // copied in loop below
    , dimension_label_map_(
          memory_tracker_->get_resource(MemoryType::DIMENSION_LABELS))
    , enumeration_map_(
          array_schema.enumeration_map_,
          memory_tracker_->get_resource(MemoryType::ENUMERATION))
    , enumeration_path_map_(
          array_schema.enumeration_path_map_,
          memory_tracker_->get_resource(MemoryType::ENUMERATION_PATHS))
    , cell_var_offsets_filters_{array_schema.cell_var_offsets_filters_}
    , cell_validity_filters_{array_schema.cell_validity_filters_}
    , coords_filters_{array_schema.coords_filters_}
    , current_domain_(array_schema.current_domain_)
    , mtx_{} {
  set_domain(array_schema.domain_);

  for (const auto& label : array_schema.dimension_labels_) {
    dimension_labels_.emplace_back(label);
    dimension_label_map_[label->name()] = label.get();
  }
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
  if (id < attributes_.size()) {
    return attributes_[id].get();
  }
  return nullptr;
}

shared_ptr<const Attribute> ArraySchema::shared_attribute(
    attribute_size_type id) const {
  if (id < attributes_.size()) {
    return attributes_[id];
  }
  return {};
}

const Attribute* ArraySchema::attribute(const std::string& name) const {
  auto it = attribute_map_.find(name);
  return it == attribute_map_.end() ? nullptr : it->second.pointer;
}

shared_ptr<const Attribute> ArraySchema::shared_attribute(
    const std::string& name) const {
  auto it = attribute_map_.find(name);
  if (it == attribute_map_.end()) {
    return {};
  }
  return attributes_[it->second.index];
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
    passert(dim_num > 0);
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
  auto attr = attribute(name);
  if (attr) {
    auto cell_val_num = attr->cell_val_num();
    return (cell_val_num == constants::var_num) ?
               constants::var_size :
               cell_val_num * datatype_size(attr->type());
  }

  // Dimension
  auto dim_it = dim_map_.find(name);
  iassert(dim_it != dim_map_.end(), "name = {}", name);
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
  auto attr{attribute(name)};
  if (attr) {
    return attr->cell_val_num();
  }

  // Dimension
  auto dim_it = dim_map_.find(name);
  iassert(dim_it != dim_map_.end(), "name = {}", name);
  return dim_it->second->cell_val_num();
}

const FilterPipeline& ArraySchema::cell_var_offsets_filters() const {
  return cell_var_offsets_filters_;
}

const FilterPipeline& ArraySchema::cell_validity_filters() const {
  return cell_validity_filters_;
}

void ArraySchema::check_webp_filter() const {
  if constexpr (webp_filter_exists) {
    WebpFilter* webp = nullptr;
    for (const auto& attr : attributes_) {
      webp = attr->filters().get_filter<WebpFilter>();
      if (webp != nullptr) {
        // WebP attributes must be of type uint8_t.
        if (attr->type() != Datatype::UINT8) {
          throw ArraySchemaException(
              "WebP filter supports only uint8 attributes");
        }
      }
    }
    // If no attribute is using WebP filter we don't need to continue checks.
    if (webp == nullptr) {
      return;
    }
    if (array_type_ != ArrayType::DENSE) {
      throw ArraySchemaException(
          "WebP filter can only be applied to dense arrays");
    }

    if (dim_map_.size() != 2) {
      throw ArraySchemaException(
          "WebP filter requires exactly 2 dimensions Y, X.");
    }
    auto y_dim = dimension_ptr(0);
    auto x_dim = dimension_ptr(1);
    if (y_dim->type() != x_dim->type()) {
      throw ArraySchemaException(
          "WebP filter dimensions 0, 1 should have matching integral types");
    }

    auto g = [&](auto T) {
      if constexpr (tiledb::type::TileDBIntegral<decltype(T)>) {
        webp->set_extents<decltype(T)>(domain_->tile_extents());
      } else {
        throw ArraySchemaException(
            "WebP filter requires integral dimensions at index 0, 1");
      }
    };
    apply_with_type(g, x_dim->type());
  }
}

void ArraySchema::check(const Config& cfg) const {
  check_without_config();
  check_enumerations(cfg);
}

void ArraySchema::check_without_config() const {
  if (domain_ == nullptr)
    throw ArraySchemaException{"Array schema check failed; Domain not set"};

  auto dim_num = this->dim_num();
  if (dim_num == 0)
    throw ArraySchemaException{
        "Array schema check failed; No dimensions provided"};

  if (cell_order_ == Layout::HILBERT && dim_num > Hilbert::HC_MAX_DIM) {
    throw ArraySchemaException{
        "Array schema check failed; Maximum dimensions supported by Hilbert "
        "order exceeded"};
  }

  if (array_type_ == ArrayType::DENSE) {
    auto type{domain_->dimension_ptr(0)->type()};
    if (datatype_is_real(type)) {
      throw ArraySchemaException{
          "Array schema check failed; Dense arrays "
          "cannot have floating point domains"};
    }
    if (attributes_.size() == 0) {
      throw ArraySchemaException{
          "Array schema check failed; No attributes provided"};
    }
  }

  if (array_type_ == ArrayType::SPARSE && capacity_ == 0) {
    throw ArraySchemaException{
        "Array schema check failed; Sparse arrays "
        "cannot have their capacity equal to zero."};
  }

  check_double_delta_compressor(coords_filters());
  check_string_compressor(coords_filters());
  check_attribute_dimension_label_names();
  check_webp_filter();

  // Check for ordered attributes on sparse arrays and arrays with more than one
  // dimension.
  if (array_type_ == ArrayType::SPARSE || this->dim_num() != 1) {
    if (has_ordered_attributes()) {
      throw ArraySchemaException{
          "Array schema check failed; Ordered attributes are only supported on "
          "dense arrays with 1 dimension."};
    }
  }

  // Check all internal dimension labels have a schema set and the schema is
  // compatible with the definition of the array it was added to.
  //
  // Note: external dimension labels do not need a schema since they are not
  // created when the array is created.
  for (auto label : dimension_labels_) {
    if (!label->is_external()) {
      if (!label->has_schema()) {
        throw ArraySchemaException{
            "Array schema check failed; Missing dimension label schema for "
            "dimension label '" +
            label->name() + "'."};
      }
      check_dimension_label_schema(label->name(), *label->schema());
    }
  }
}

void ArraySchema::check_dimension_label_schema(
    const std::string& name, const ArraySchema& schema) const {
  // Check there is a dimension label with the requested name and get the
  // dimension label reference for it.
  auto dim_iter = dimension_label_map_.find(name);
  if (dim_iter == dimension_label_map_.end()) {
    throw ArraySchemaException(
        "No dimension label with the name '" + name + "'.");
  }
  const auto* dim_label_ref = dim_iter->second;

  // Check there is only one dimension in the provided schema.
  if (schema.dim_num() != 1) {
    throw ArraySchemaException(
        "Invalid schema for label '" + name + "'; Schema has " +
        std::to_string(schema.dim_num()) + " dimensions.");
  }

  // Check the dimension in the schema matches the local dimension.
  const auto* dim_internal = dimension_ptr(dim_label_ref->dimension_index());
  const auto* dim_provided = schema.dimension_ptr(0);
  if (dim_provided->type() != dim_internal->type()) {
    throw ArraySchemaException(
        "The dimension datatype of the dimension label is '" +
        datatype_str(dim_provided->type()) + "', but expected datatype was '" +
        datatype_str(dim_internal->type()) + "'");
  }
  if (dim_provided->cell_val_num() != dim_internal->cell_val_num()) {
    throw ArraySchemaException(
        "The cell value number of the dimension in the dimension label is " +
        std::to_string(dim_provided->cell_val_num()) +
        ", but the expected datatype was " +
        std::to_string(dim_internal->cell_val_num()) + ".");
  }

  // Check there is an attribute the schema with the label attribute name.
  const auto& label_attr_name = dim_label_ref->label_attr_name();
  if (!schema.is_attr(label_attr_name)) {
    throw ArraySchemaException(
        "The dimension label is missing an attribute with name '" +
        label_attr_name + "'.");
  }

  // Check the label attribute matches the expected attribute.
  const auto* label_attr = schema.attribute(label_attr_name);
  if (label_attr->order() != dim_label_ref->label_order()) {
    throw ArraySchemaException(
        "The label order of the dimension label is " +
        data_order_str(label_attr->order()) +
        ", but the expected label order was " +
        data_order_str(dim_label_ref->label_order()) + ".");
  }
  if (label_attr->type() != dim_label_ref->label_type()) {
    throw ArraySchemaException(
        "The datatype of the dimension label is " +
        datatype_str(label_attr->type()) +
        ", but the expected label datatype was " +
        datatype_str(dim_label_ref->label_type()) + ".");
  }
  if (label_attr->cell_val_num() != dim_label_ref->label_cell_val_num()) {
    throw ArraySchemaException(
        "The cell value number of the label attribute in the dimension label " +
        std::to_string(label_attr->cell_val_num()) +
        ", but the expected cell value number was " +
        std::to_string(dim_label_ref->label_cell_val_num()) + ".");
  }
}

void ArraySchema::check_enumerations(const Config& cfg) const {
  auto max_size = cfg.get<uint64_t>("sm.enumerations_max_size");
  if (!max_size.has_value()) {
    throw std::runtime_error(
        "Missing required config parameter 'sm.enumerations_max_size'.");
  }

  auto max_total_size = cfg.get<uint64_t>("sm.enumerations_max_total_size");
  if (!max_total_size.has_value()) {
    throw std::runtime_error(
        "Missing required config parameter 'sm.enumerations_max_total_size'.");
  }

  uint64_t total_size = 0;
  for (const auto& pair : enumeration_map_) {
    if (!pair.second) {
      // We don't have an Array instance at this point so the best we can do
      // is just avoid segfaulting when we attempt to check with unloaded
      // enumerations.
      continue;
    }
    uint64_t size = pair.second->data().size() + pair.second->offsets().size();
    if (size > max_size.value()) {
      throw ArraySchemaException(
          "Invalid enumeration '" + pair.second->name() +
          "' has a size "
          "exceeding " +
          std::to_string(max_size.value()) + " bytes.");
    }
    total_size += size;
  }

  if (total_size > max_total_size.value()) {
    throw ArraySchemaException(
        "Total enumeration size for the entire schema "
        "exceeds " +
        std::to_string(max_total_size.value()) + " bytes.");
  }
}

const FilterPipeline& ArraySchema::filters(const std::string& name) const {
  if (is_special_attribute(name)) {
    return coords_filters();
  }

  // Attribute
  auto attr{attribute(name)};
  if (attr) {
    return attr->filters();
  }

  // Dimension (if filters not set, return default coordinate filters)
  auto dim_it = dim_map_.find(name);
  iassert(dim_it != dim_map_.end(), "name = {}", name);
  const auto& ret = dim_it->second->filters();
  return ret;
}

const FilterPipeline& ArraySchema::coords_filters() const {
  return coords_filters_;
}

bool ArraySchema::dense() const {
  return array_type_ == ArrayType::DENSE;
}

const DimensionLabel& ArraySchema::dimension_label(
    const dimension_label_size_type i) const {
  return *dimension_labels_[i];
}

const DimensionLabel& ArraySchema::dimension_label(
    const std::string& name) const {
  auto iter = dimension_label_map_.find(name);
  if (iter == dimension_label_map_.end())
    throw ArraySchemaException(
        "Unable to get dimension label reference; No dimension label named '" +
        name + "'.");
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

void ArraySchema::has_attribute(const std::string& name, bool* has_attr) const {
  *has_attr = false;

  for (auto& attr : attributes_) {
    if (name == attr->name()) {
      *has_attr = true;
      break;
    }
  }
}

bool ArraySchema::has_ordered_attributes() const {
  return std::any_of(
      attributes_.cbegin(), attributes_.cend(), [](const auto& attr) {
        return attr->order() != DataOrder::UNORDERED_DATA;
      });
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
  return is_attr(name) || is_dim(name) || is_dim_label(name) ||
         is_special_attribute(name);
}

bool ArraySchema::is_nullable(const std::string& name) const {
  auto attr = this->attribute(name);
  if (attr == nullptr)
    return false;
  return attr->nullable();
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
  auto attr{attribute(name)};
  if (attr) {
    return attr->type();
  }

  // Dimension
  auto dim_it = dim_map_.find(name);
  iassert(dim_it != dim_map_.end(), "name = {}", name);
  return dim_it->second->type();
}

bool ArraySchema::var_size(const std::string& name) const {
  // Special case for zipped coordinates
  if (is_special_attribute(name)) {
    return false;
  }

  // Attribute
  auto attr{attribute(name)};
  if (attr) {
    return attr->var_size();
  }

  // Dimension
  auto dim_it = dim_map_.find(name);
  if (dim_it != dim_map_.end()) {
    return dim_it->second->var_size();
  }

  // Dimension label
  auto dim_label_ref_it = dimension_label_map_.find(name);
  iassert(dim_label_ref_it != dimension_label_map_.end(), "name = {}", name);
  return dim_label_ref_it->second->is_var();
}

void ArraySchema::add_attribute(
    shared_ptr<const Attribute> attr, bool check_special) {
  // Sanity check
  if (attr == nullptr) {
    throw ArraySchemaException("Cannot add attribute; Input attribute is null");
  }

  // Do not allow attributes with special names
  if (check_special && attr->name().find(constants::special_name_prefix) == 0) {
    std::string msg = "Cannot add attribute; Attribute names starting with '";
    msg += std::string(constants::special_name_prefix) + "' are reserved";
    throw ArraySchemaException(msg);
  }

  auto enmr_name = attr->get_enumeration_name();
  if (enmr_name.has_value()) {
    // The referenced enumeration must exist when the attribut is added
    auto iter = enumeration_map_.find(enmr_name.value());
    if (iter == enumeration_map_.end()) {
      std::string msg =
          "Cannot add attribute; Attribute refers to an "
          "unknown enumeration named '" +
          enmr_name.value() + "'.";
      throw ArraySchemaException(msg);
    }

    // This attribute must have an integral datatype to support Enumerations
    if (!datatype_is_integer(attr->type())) {
      std::string msg = "Unable to use enumeration with attribute '" +
                        attr->name() +
                        "', attribute must have an integral data type, not " +
                        datatype_str(attr->type());
      throw ArraySchemaException(msg);
    }

    // The attribute must have a cell_val_num of 1
    if (attr->cell_val_num() != 1) {
      std::string msg =
          "Attributes with enumerations must have a cell_val_num of 1.";
      throw ArraySchemaException(msg);
    }

    auto enmr = get_enumeration(enmr_name.value());
    if (enmr == nullptr) {
      throw ArraySchemaException(
          "Cannot add attribute referencing enumeration '" + enmr_name.value() +
          "' as the enumeration has not been loaded.");
    }

    // The +1 here is because of 0 being a valid index into the enumeration.
    if (datatype_max_integral_value(attr->type()) <= enmr->elem_count()) {
      throw ArraySchemaException(
          "Unable to use enumeration '" + enmr_name.value() +
          "' for attribute '" + attr->name() +
          "' because the attribute's type is not large enough to represent "
          "all enumeration values.");
    }
  }

  // Create new attribute and potentially set a default name
  auto k{static_cast<unsigned int>(attributes_.size())};
  attributes_.emplace_back(attr);
  attribute_map_[attr->name()] = {attr.get(), k};
}

void ArraySchema::add_dimension_label(
    dimension_size_type dim_id,
    const std::string& name,
    DataOrder label_order,
    Datatype label_type,
    bool check_name) {
  // Check the label order is valid.
  if (label_order == DataOrder::UNORDERED_DATA) {
    throw ArraySchemaException(
        "Cannot add dimension label; Unordered dimension labels are not yet "
        "supported.");
  }

  // Check domain is set and `dim_id` is a valid dimension index.
  if (!domain_) {
    throw ArraySchemaException(
        "Cannot add dimension label; Must set domain before adding dimension "
        "labels.");
  }
  if (dim_id >= domain_->dim_num()) {
    throw ArraySchemaException(
        "Cannot add a label to dimension " + std::to_string(dim_id) +
        "; Invalid dimension index. ");
  }

  // Get the dimension the dimension label will be added to.
  auto dim = domain_->dimension_ptr(dim_id);

  // Check the dimension label is unique among attribute, dimension, and label
  // names.
  if (check_name) {
    // Check no attribute with this name
    bool has_matching_name{false};
    has_attribute(name, &has_matching_name);
    if (has_matching_name) {
      throw ArraySchemaException(
          "Cannot add a dimension label with name '" + std::string(name) +
          "'. An attribute with that name already exists.");
    }

    // Check no dimension with this name
    domain_->has_dimension(name, &has_matching_name);
    if (has_matching_name) {
      throw ArraySchemaException(
          "Cannot add a dimension label with name '" + std::string(name) +
          "'. A dimension with that name already exists.");
    }

    // Check no other dimension label with this name.
    auto found = dimension_label_map_.find(name);
    if (found != dimension_label_map_.end()) {
      throw ArraySchemaException(
          "Cannot add a dimension label with name '" + std::string(name) +
          "'. A different label with that name already exists.");
    }
  }

  // Add dimension label
  try {
    // Create relative URI in dimension label directory
    URI uri{
        constants::array_dimension_labels_dir_name + "/l" +
            std::to_string(nlabel_internal_),
        false};

    // Create the dimension label reference.
    auto dim_label_ref = make_shared<DimensionLabel>(
        HERE(),
        dim_id,
        name,
        uri,
        dim,
        label_order,
        label_type,
        memory_tracker_);
    dimension_labels_.emplace_back(dim_label_ref);
    dimension_label_map_[name] = dim_label_ref.get();
  } catch (...) {
    std::throw_with_nested(
        ArraySchemaException("Failed to add dimension label '" + name + "'."));
  }
  ++nlabel_internal_;  // WARNING: not atomic
}

void ArraySchema::drop_attribute(const std::string& attr_name) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (attr_name.empty()) {
    throw ArraySchemaException("Cannot remove an empty name attribute");
  }

  if (!attribute(attr_name)) {
    // Not exists.
    throw ArraySchemaException("Cannot remove a non-exist attribute");
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
}

void ArraySchema::add_enumeration(shared_ptr<const Enumeration> enmr) {
  if (enmr == nullptr) {
    throw ArraySchemaException(
        "Error adding enumeration. Enumeration "
        "must not be nullptr.");
  }

  if (enumeration_map_.find(enmr->name()) != enumeration_map_.end()) {
    throw ArraySchemaException(
        "Error adding enumeration. Enumeration with name '" + enmr->name() +
        "' already exists in this ArraySchema.");
  }

  enumeration_map_[enmr->name()] = enmr;
  enumeration_path_map_[enmr->name()] = enmr->path_name();
}

void ArraySchema::extend_enumeration(shared_ptr<const Enumeration> enmr) {
  if (enmr == nullptr) {
    throw ArraySchemaException(
        "Error adding enumeration. Enumeration must not be nullptr.");
  }

  auto it = enumeration_map_.find(enmr->name());
  if (it == enumeration_map_.end()) {
    throw ArraySchemaException(
        "Error extending enumeration. Enumeration with name '" + enmr->name() +
        "' does not exist in this ArraySchema.");
  }

  if (it->second == nullptr) {
    throw ArraySchemaException(
        "Error extending enumeration. Enumeration with name '" + enmr->name() +
        "' is not loaded.");
  }

  if (!enmr->is_extension_of(it->second)) {
    throw ArraySchemaException(
        "Error extending enumeration. Provided enumeration is not an extension "
        "of the current state of '" +
        enmr->name() + "'");
  }

  if (enumeration_path_map_.find(enmr->name()) == enumeration_path_map_.end()) {
    throw ArraySchemaException(
        "Error extending enumeration. Invalid enumeration path map state for "
        "enumeration '" +
        enmr->name() + "'");
  }

  for (auto& enmr_path : enumeration_path_map_) {
    if (enmr->path_name() == enmr_path.second) {
      throw ArraySchemaException(
          "Error extending enumeration. Enumeration path name for '" +
          enmr->name() + "' already exists in this schema.");
    }
  }

  enumeration_map_[enmr->name()] = enmr;
  enumeration_path_map_[enmr->name()] = enmr->path_name();
}

void ArraySchema::store_enumeration(shared_ptr<const Enumeration> enmr) {
  if (enmr == nullptr) {
    throw ArraySchemaException(
        "Error storing enumeration. Enumeration must not be nullptr.");
  }

  auto name_iter = enumeration_map_.find(enmr->name());
  if (name_iter == enumeration_map_.end()) {
    throw ArraySchemaException(
        "Error storing enumeration. Unknown enumeration name '" + enmr->name() +
        "'.");
  }

  if (name_iter->second != nullptr) {
    throw ArraySchemaException(
        "Error storing enumeration. Enumeration named '" + enmr->name() +
        "' has already been stored.");
  }

  auto path_iter = enumeration_path_map_.find(enmr->name());
  if (path_iter == enumeration_path_map_.end()) {
    throw ArraySchemaException(
        "Error storing enumeration. Missing path name map entry.");
  }

  if (path_iter->second != enmr->path_name()) {
    throw ArraySchemaException(
        "Error storing enumeration. Path name mismatch for enumeration "
        "named '" +
        enmr->name() + "'.");
  }

  name_iter->second = enmr;
}

bool ArraySchema::has_enumeration(const std::string& enmr_name) const {
  return enumeration_map_.find(enmr_name) != enumeration_map_.end();
}

std::vector<std::string> ArraySchema::get_enumeration_names() const {
  std::vector<std::string> enmr_names;
  for (auto& entry : enumeration_path_map_) {
    enmr_names.emplace_back(entry.first);
  }
  return enmr_names;
}

std::vector<std::string> ArraySchema::get_loaded_enumeration_names() const {
  std::vector<std::string> enmr_names;
  for (auto& entry : enumeration_map_) {
    if (entry.second != nullptr) {
      enmr_names.emplace_back(entry.first);
    }
  }
  return enmr_names;
}

std::vector<shared_ptr<const Enumeration>>
ArraySchema::get_loaded_enumerations() const {
  std::vector<shared_ptr<const Enumeration>> enmrs;
  for (auto& entry : enumeration_map_) {
    if (entry.second != nullptr) {
      enmrs.emplace_back(entry.second);
    }
  }
  return enmrs;
}

bool ArraySchema::is_enumeration_loaded(
    const std::string& enumeration_name) const {
  auto iter = enumeration_map_.find(enumeration_name);

  if (iter == enumeration_map_.end()) {
    throw ArraySchemaException(
        "Unable to check if unknown enumeration is loaded. No enumeration "
        "named '" +
        enumeration_name + "'.");
  }

  return iter->second != nullptr;
}

shared_ptr<const Enumeration> ArraySchema::get_enumeration(
    const std::string& enmr_name) const {
  auto iter = enumeration_map_.find(enmr_name);
  if (iter == enumeration_map_.end()) {
    throw ArraySchemaException(
        "Unable to get enumeration. Unknown enumeration named '" + enmr_name +
        "'.");
  }

  return iter->second;
}

const std::string& ArraySchema::get_enumeration_path_name(
    const std::string& enmr_name) const {
  auto iter = enumeration_path_map_.find(enmr_name);
  if (iter == enumeration_path_map_.end()) {
    throw ArraySchemaException(
        "Unable to get enumeration path name. Unknown enumeration named '" +
        enmr_name + "'.");
  }

  return iter->second;
}

void ArraySchema::drop_enumeration(const std::string& enmr_name) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (enmr_name.empty()) {
    throw ArraySchemaException(
        "Error dropping enumeration, empty names are invalid.");
  }

  auto it = enumeration_map_.find(enmr_name);
  if (it == enumeration_map_.end()) {
    throw ArraySchemaException(
        "Error dropping enumeration, no enumeration named '" + enmr_name +
        "'.");
  }

  for (auto attr : attributes_) {
    auto attr_enmr_name = attr->get_enumeration_name();
    if (!attr_enmr_name.has_value()) {
      continue;
    }
    if (attr_enmr_name.value() == enmr_name) {
      throw ArraySchemaException(
          "Unable to drop enumeration '" + enmr_name + "' as it is used by" +
          " attribute '" + attr->name() + "'.");
    }
  }

  // Drop from both the path and name maps.
  enumeration_path_map_.erase(it->first);
  enumeration_map_.erase(it);
}

// #TODO Add security validation on incoming URI
shared_ptr<ArraySchema> ArraySchema::deserialize(
    Deserializer& deserializer,
    const URI& uri,
    shared_ptr<MemoryTracker> memory_tracker) {
  Status st;
  // Load version
  // #TODO Add security validation
  auto version = deserializer.read<uint32_t>();
  if (version > constants::format_version) {
    if constexpr (!is_experimental_build) {
      auto base_version =
          (version & (1 << 31)) ? (version | (0 << 31)) : version;
      if (base_version > constants::format_version) {
        throw ArraySchemaException(
            "Failed to deserialize; Array format version (" +
            std::to_string(base_version) +
            ") is newer than the library format version (" +
            std::to_string(constants::format_version) + ")");
      }
    }
    throw ArraySchemaException(
        "Failed to deserialize; Array format version (" +
        std::to_string(version) +
        ") is newer than the library format version (" +
        std::to_string(constants::format_version) + ")");
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
  auto coords_filters{
      FilterPipeline::deserialize(deserializer, version, Datatype::UINT64)};
  auto cell_var_filters{
      FilterPipeline::deserialize(deserializer, version, Datatype::UINT64)};
  FilterPipeline cell_validity_filters;
  if (version >= 7) {
    cell_validity_filters =
        FilterPipeline::deserialize(deserializer, version, Datatype::UINT8);
  }

  // Load domain
  // Note: Security validation delegated to invoked API
  // #TODO Add security validation
  auto domain{Domain::deserialize(
      deserializer,
      version,
      cell_order,
      tile_order,
      coords_filters,
      memory_tracker)};

  // Load attributes
  // Note: Security validation delegated to invoked API
  // #TODO Add security validation
  std::vector<shared_ptr<const Attribute>> attributes;
  uint32_t attribute_num = deserializer.read<uint32_t>();
  for (uint32_t i = 0; i < attribute_num; ++i) {
    auto attr{Attribute::deserialize(deserializer, version)};
    attributes.emplace_back(make_shared<Attribute>(HERE(), move(attr)));
  }

  // Load dimension labels
  std::vector<shared_ptr<const DimensionLabel>> dimension_labels;
  if (version >= 18) {
    uint32_t label_num = deserializer.read<uint32_t>();
    for (uint32_t i{0}; i < label_num; ++i) {
      dimension_labels.emplace_back(
          DimensionLabel::deserialize(deserializer, version));
    }
  }

  // Load enumeration name to path map
  std::unordered_map<std::string, std::string> enumeration_path_map;
  if (version >= constants::enumerations_min_format_version) {
    uint32_t enmr_num = deserializer.read<uint32_t>();
    for (uint32_t i = 0; i < enmr_num; i++) {
      auto enmr_name_size = deserializer.read<uint32_t>();
      std::string enmr_name(
          deserializer.get_ptr<char>(enmr_name_size), enmr_name_size);

      auto enmr_filename_size = deserializer.read<uint32_t>();
      std::string enmr_filename(
          deserializer.get_ptr<char>(enmr_filename_size), enmr_filename_size);

      enumeration_path_map[enmr_name] = enmr_filename;
    }
  }

  // Load the array current domain, if this is an older array, it'll get by
  // default an empty current domain object
  auto current_domain = make_shared<CurrentDomain>(
      memory_tracker, constants::current_domain_version);
  if (version >= constants::current_domain_min_format_version) {
    current_domain =
        CurrentDomain::deserialize(deserializer, memory_tracker, domain);
  }

  // Validate
  if (cell_order == Layout::HILBERT &&
      domain->dim_num() > Hilbert::HC_MAX_DIM) {
    throw ArraySchemaException(
        "Array schema check failed; Maximum dimensions supported by Hilbert "
        "order exceeded");
  }

  if (array_type == ArrayType::DENSE) {
    auto type{domain->dimension_ptr(0)->type()};
    if (datatype_is_real(type)) {
      throw ArraySchemaException(
          "Array schema check failed; Dense arrays cannot have floating "
          "point domains");
    }
    if (attributes.size() == 0) {
      throw ArraySchemaException(
          "Array schema check failed; No attributes provided");
    }
  }

  // Populate timestamp range
  FragmentID fragment_id{uri};
  auto timestamp_range{fragment_id.timestamp_range()};

  // Set schema name
  std::string name = uri.last_path_part();

  return make_shared<ArraySchema>(
      HERE(),
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
      std::vector<shared_ptr<const Enumeration>>(),
      enumeration_path_map,
      cell_var_filters,
      cell_validity_filters,
      FilterPipeline(
          coords_filters,
          version < 5 ? domain->dimension_ptr(0)->type() : Datatype::UINT64),
      current_domain,
      memory_tracker);
}

shared_ptr<ArraySchema> ArraySchema::clone() const {
  return make_shared<ArraySchema>(HERE(), *this);
}

void ArraySchema::set_allows_dups(bool allows_dups) {
  if (allows_dups && array_type_ == ArrayType::DENSE) {
    throw ArraySchemaException(
        "Dense arrays cannot allow coordinate duplicates");
  }

  allows_dups_ = allows_dups;
}

void ArraySchema::set_array_uri(const URI& array_uri) {
  array_uri_ = array_uri;
}

void ArraySchema::set_name(const std::string& name) {
  name_ = name;
}

void ArraySchema::set_capacity(uint64_t capacity) {
  if (array_type_ == ArrayType::SPARSE && capacity == 0) {
    throw ArraySchemaException(
        "Sparse arrays cannot have their capacity equal to zero.");
  }

  capacity_ = capacity;
}

void ArraySchema::set_coords_filter_pipeline(const FilterPipeline& pipeline) {
  check_string_compressor(pipeline);
  check_double_delta_compressor(pipeline);
  coords_filters_ = pipeline;
}

void ArraySchema::set_cell_var_offsets_filter_pipeline(
    const FilterPipeline& pipeline) {
  cell_var_offsets_filters_ = pipeline;
}

void ArraySchema::set_cell_order(Layout cell_order) {
  if (dense() && cell_order == Layout::HILBERT) {
    throw ArraySchemaException(
        "Cannot set cell order; Hilbert order is only "
        "applicable to sparse arrays");
  }

  if (cell_order == Layout::UNORDERED) {
    throw ArraySchemaException(
        "Cannot set cell order; Cannot create ArraySchema "
        "with UNORDERED cell order");
  }

  cell_order_ = cell_order;
}

void ArraySchema::set_cell_validity_filter_pipeline(
    const FilterPipeline& pipeline) {
  cell_validity_filters_ = pipeline;
}

void ArraySchema::set_dimension_label_filter_pipeline(
    const std::string& label_name, const FilterPipeline& pipeline) {
  auto& dim_label_ref = dimension_label(label_name);
  if (!dim_label_ref.has_schema()) {
    throw ArraySchemaException(
        "Cannot set filter pipeline for dimension label '" + label_name +
        "'; No dimension label schema is set.");
  }
  const_cast<Attribute*>(
      dim_label_ref.schema()->attribute(dim_label_ref.label_attr_name()))
      ->set_filter_pipeline(pipeline);
}

void ArraySchema::set_dimension_label_tile_extent(
    const std::string& label_name,
    const Datatype type,
    const void* tile_extent) {
  auto& dim_label_ref = dimension_label(label_name);
  if (!dim_label_ref.has_schema()) {
    throw ArraySchemaException(
        "Cannot set tile extent for dimension label '" + label_name +
        "'; No dimension label schema is set.");
  }
  auto dim = dim_label_ref.schema()->dimension_ptr(0);
  if (type != dim->type()) {
    throw ArraySchemaException(
        "Cannot set tile extent for dimension label '" + label_name +
        "; The dimension the label is set on has type '" +
        datatype_str(dim->type()) +
        "'which does not match the provided datatype '" + datatype_str(type) +
        "'.");
  }
  const_cast<Dimension*>(dim)->set_tile_extent(tile_extent);
}

void ArraySchema::set_domain(shared_ptr<Domain> domain) {
  if (domain == nullptr) {
    throw ArraySchemaException("Cannot set domain; Input domain is nullptr");
  }

  if (domain->dim_num() == 0) {
    throw ArraySchemaException(
        "Cannot set domain; Domain must contain at least one dimension");
  }

  if (array_type_ == ArrayType::DENSE) {
    if (!domain->all_dims_same_type()) {
      throw ArraySchemaException(
          "Cannot set domain; In dense arrays, all "
          "dimensions must have the same datatype");
    }

    auto type{domain->dimension_ptr(0)->type()};
    if (!datatype_is_integer(type) && !datatype_is_datetime(type) &&
        !datatype_is_time(type)) {
      throw ArraySchemaException(
          std::string("Cannot set domain; Dense arrays "
                      "do not support dimension datatype '") +
          datatype_str(type) + "'");
    }
  }

  if (cell_order_ != Layout::HILBERT) {
    domain->set_null_tile_extents_to_range();
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
}

void ArraySchema::set_tile_order(Layout tile_order) {
  if (tile_order == Layout::HILBERT) {
    throw ArraySchemaException(
        "Cannot set tile order; Hilbert order is not applicable to tiles");
  }

  if (tile_order == Layout::UNORDERED) {
    throw ArraySchemaException(
        "Cannot set tile order; Cannot create ArraySchema "
        "with UNORDERED tile order");
  }

  tile_order_ = tile_order;
}

void ArraySchema::set_version(format_version_t version) {
  version_ = version;
}

format_version_t ArraySchema::write_version() const {
  return version_ < constants::back_compat_writes_min_format_version ?
             constants::format_version :
             version_;
}

format_version_t ArraySchema::version() const {
  return version_;
}

void ArraySchema::set_timestamp_range(
    const std::pair<uint64_t, uint64_t>& timestamp_range) {
  timestamp_range_ = timestamp_range;
}

std::pair<uint64_t, uint64_t> ArraySchema::timestamp_range() const {
  return std::pair<uint64_t, uint64_t>(
      timestamp_range_.first, timestamp_range_.second);
}

uint64_t ArraySchema::timestamp_start() const {
  return timestamp_range_.first;
}

uint64_t ArraySchema::timestamp_end() const {
  return timestamp_range_.second;
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

void ArraySchema::check_attribute_dimension_label_names() const {
  std::set<std::string> names;
  // Check attribute and dimension names are unique.
  auto dim_num = this->dim_num();
  uint64_t expected_unique_names{
      dim_num + attributes_.size() + dimension_labels_.size()};
  for (const auto& attr : attributes_) {
    names.insert(attr->name());
  }
  for (dimension_size_type i = 0; i < dim_num; ++i) {
    names.insert(domain_->dimension_ptr(i)->name());
  }
  for (const auto& dim_label_ref : dimension_labels_) {
    names.insert(dim_label_ref->name());
  }
  if (names.size() != expected_unique_names) {
    throw ArraySchemaException(
        "Array schema check failed; Attributes, dimensions and dimension "
        "labels must have unique names");
  }
}

void ArraySchema::check_double_delta_compressor(
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
  if (!has_double_delta) {
    return;
  }

  // Error if any real dimension inherits the coord filters with DOUBLE DELTA.
  // A dimension inherits the filters when it has no filters.
  auto dim_num = domain_->dim_num();
  for (dimension_size_type d = 0; d < dim_num; ++d) {
    auto dim{domain_->dimension_ptr(d)};
    const auto& dim_filters = dim->filters();
    auto dim_type = dim->type();
    if (datatype_is_real(dim_type) && dim_filters.empty())
      throw ArraySchemaException(
          "Real dimension cannot inherit coordinate "
          "filters with DOUBLE DELTA compression");
  }
}

void ArraySchema::check_string_compressor(const FilterPipeline& filters) const {
  // There is no error if only 1 filter is used
  if (filters.size() <= 1 ||
      !(filters.has_filter(FilterType::FILTER_RLE) ||
        filters.has_filter(FilterType::FILTER_DICTIONARY))) {
    return;
  }

  // If RLE or Dictionary-encoding is set for strings, they need to be
  // the first filter in the list
  auto dim_num = domain_->dim_num();
  for (dimension_size_type d = 0; d < dim_num; ++d) {
    auto dim{domain_->dimension_ptr(d)};
    const auto& dim_filters = dim->filters();
    // if it's a var-length string dimension and there is no specific filter
    // list already set for that dimension (then coords_filters_ will be used)
    if (dim->type() == Datatype::STRING_ASCII && dim->var_size() &&
        dim_filters.empty()) {
      if (filters.has_filter(FilterType::FILTER_RLE) &&
          filters.get_filter(0)->type() != FilterType::FILTER_RLE) {
        throw ArraySchemaException(
            "RLE filter must be the first filter to apply when used on "
            "variable length string dimensions");
      }
      if (filters.has_filter(FilterType::FILTER_DICTIONARY) &&
          filters.get_filter(0)->type() != FilterType::FILTER_DICTIONARY) {
        throw ArraySchemaException(
            "Dictionary filter must be the first filter to apply when used on "
            "variable length string dimensions");
      }
    }
  }
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

void ArraySchema::generate_uri(
    std::optional<std::pair<uint64_t, uint64_t>> timestamp_range) {
  if (timestamp_range == std::nullopt) {
    auto timestamp = utils::time::timestamp_now_ms();
    timestamp_range_ = std::make_pair(timestamp, timestamp);
  } else {
    timestamp_range_ = timestamp_range.value();
  }

  name_ = tiledb::storage_format::generate_timestamped_name(
      timestamp_range_.first, timestamp_range_.second, std::nullopt);
  uri_ =
      array_uri_.join_path(constants::array_schema_dir_name).join_path(name_);
}

void ArraySchema::expand_current_domain(
    shared_ptr<CurrentDomain> new_current_domain) {
  if (new_current_domain == nullptr) {
    throw ArraySchemaException(
        "The argument specified for current domain expansion is nullptr.");
  }

  // Check that the new current domain expands the existing one and not shrinks
  // it. Every current domain covers an empty current domain.
  if (!current_domain_->empty() &&
      !current_domain_->covered(new_current_domain)) {
    throw ArraySchemaException(
        "The current domain of an array can only be expanded, please adjust "
        "your new current domain object.");
  }

  new_current_domain->ndrectangle()->set_domain(this->shared_domain());
  new_current_domain->check_schema_sanity(this->shared_domain());

  current_domain_ = new_current_domain;
}

shared_ptr<CurrentDomain> ArraySchema::get_current_domain() const {
  return current_domain_;
}

void ArraySchema::set_current_domain(shared_ptr<CurrentDomain> current_domain) {
  if (current_domain == nullptr) {
    throw ArraySchemaException(
        "The argument specified for setting the current domain on the "
        "schema is nullptr.");
  }
  current_domain_ = current_domain;
}

}  // namespace tiledb::sm

std::ostream& operator<<(
    std::ostream& os, const tiledb::sm::ArraySchema& schema) {
  os << "- Array type: " << array_type_str(schema.array_type()) << std::endl;
  os << "- Cell order: " << layout_str(schema.cell_order()) << std::endl;
  os << "- Tile order: " << layout_str(schema.tile_order()) << std::endl;
  os << "- Capacity: " << schema.capacity() << std::endl;
  os << "- Allows duplicates: " << (schema.allows_dups() ? "true" : "false")
     << std::endl;
  os << "- Coordinates filters: " << schema.coords_filters().size();

  os << schema.coords_filters();

  os << std::endl
     << "- Offsets filters: " << schema.cell_var_offsets_filters().size();

  os << schema.cell_var_offsets_filters();
  os << std::endl
     << "- Validity filters: " << schema.cell_validity_filters().size();

  os << schema.cell_validity_filters();

  if (schema.shared_domain() != nullptr) {
    os << std::endl;
    os << *schema.shared_domain();
  }

  for (auto& attr : schema.attributes()) {
    os << std::endl;
    os << *attr;
  }

  for (auto& enmr_iter : schema.enumeration_map()) {
    os << std::endl;
    if (enmr_iter.second != nullptr) {
      os << *enmr_iter.second;
    } else {
      os << "### Enumeration ###" << std::endl;
      os << "- Name: " << enmr_iter.first << std::endl;
      os << "- Loaded: false" << std::endl;
    }
  }

  for (auto& label : schema.dimension_labels()) {
    os << std::endl;
    os << *label;
  }

  os << std::endl;
  os << *schema.get_current_domain();

  return os;
}
