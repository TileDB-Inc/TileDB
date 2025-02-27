/**
 * @file   array_schema_evolution.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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
 * This file implements the ArraySchemaEvolution class.
 */

#include "tiledb/sm/array_schema/array_schema_evolution.h"
#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/current_domain.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/array_schema/enumeration.h"
#include "tiledb/sm/buffer/buffer.h"
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

namespace tiledb::sm {

/** Class for locally generated exceptions. */
class ArraySchemaEvolutionException : public StatusException {
 public:
  explicit ArraySchemaEvolutionException(const std::string& msg)
      : StatusException("ArraySchemaEvolution", msg) {
  }
};

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArraySchemaEvolution::ArraySchemaEvolution(
    shared_ptr<MemoryTracker> memory_tracker)
    : memory_tracker_(memory_tracker)
    , attributes_to_add_(memory_tracker->get_resource(MemoryType::ATTRIBUTES))
    , enumerations_to_add_map_(
          memory_tracker_->get_resource(MemoryType::ENUMERATION))
    , enumerations_to_extend_map_(
          memory_tracker_->get_resource(MemoryType::ENUMERATION)) {
}

ArraySchemaEvolution::ArraySchemaEvolution(
    std::vector<shared_ptr<Attribute>> attrs_to_add,
    std::unordered_set<std::string> attrs_to_drop,
    std::unordered_map<std::string, shared_ptr<const Enumeration>> enmrs_to_add,
    std::unordered_map<std::string, shared_ptr<const Enumeration>>
        enmrs_to_extend,
    std::unordered_set<std::string> enmrs_to_drop,
    std::pair<uint64_t, uint64_t> timestamp_range,
    shared_ptr<CurrentDomain> current_domain,
    shared_ptr<MemoryTracker> memory_tracker)
    : memory_tracker_(memory_tracker)
    , attributes_to_add_(memory_tracker->get_resource(MemoryType::ATTRIBUTES))
    , attributes_to_drop_(attrs_to_drop)
    , enumerations_to_add_map_(
          memory_tracker_->get_resource(MemoryType::ENUMERATION))
    , enumerations_to_extend_map_(
          memory_tracker_->get_resource(MemoryType::ENUMERATION))
    , enumerations_to_drop_(enmrs_to_drop)
    , timestamp_range_(timestamp_range)
    , current_domain_to_expand_(current_domain) {
  for (auto& elem : attrs_to_add) {
    attributes_to_add_.push_back(elem);
  }

  for (auto& elem : enmrs_to_add) {
    enumerations_to_add_map_.insert(elem);
  }

  for (auto& elem : enmrs_to_extend) {
    enumerations_to_extend_map_.insert(elem);
  }
}

ArraySchemaEvolution::~ArraySchemaEvolution() {
  clear();
}

/* ****************************** */
/*               API              */
/* ****************************** */

shared_ptr<ArraySchema> ArraySchemaEvolution::evolve_schema(
    const shared_ptr<const ArraySchema>& orig_schema) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (orig_schema == nullptr) {
    throw ArraySchemaEvolutionException(
        "Cannot evolve schema; Input array schema is null");
  }

  auto schema = orig_schema->clone();

  // Add enumerations. Must be done before attributes so that any attributes
  // referencing enumerations won't fail to be added.
  for (auto& enmr : enumerations_to_add_map_) {
    schema->add_enumeration(enmr.second);
  }

  for (auto& enmr : enumerations_to_extend_map_) {
    schema->extend_enumeration(enmr.second);
  }

  // Add attributes.
  for (auto& attr : attributes_to_add_) {
    throw_if_not_ok(schema->add_attribute(attr, false));
  }

  // Drop attributes.
  for (auto& attr_name : attributes_to_drop_) {
    bool has_attr = false;
    throw_if_not_ok(schema->has_attribute(attr_name, &has_attr));
    if (!has_attr) {
      throw ArraySchemaEvolutionException(
          "Cannot drop attribute; Input attribute name refers to a dimension "
          "or does not exist");
    }
    throw_if_not_ok(schema->drop_attribute(attr_name));
  }

  // Drop enumerations
  for (auto& enmr_name : enumerations_to_drop_) {
    if (schema->has_enumeration(enmr_name)) {
      schema->drop_enumeration(enmr_name);
    }
  }

  // Set timestamp, if specified
  if (std::get<0>(timestamp_range_) != 0) {
    schema->generate_uri(timestamp_range_);
  } else {
    // Generate new schema URI
    schema->generate_uri();
  }

  // Get expanded current domain
  if (current_domain_to_expand_) {
    schema->expand_current_domain(current_domain_to_expand_);
  }

  return schema;
}

void ArraySchemaEvolution::add_attribute(shared_ptr<const Attribute> attr) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (!attr) {
    throw ArraySchemaEvolutionException(
        "Cannot add attribute; Input attribute is null");
  }
  for (const auto& a : attributes_to_add_) {
    if (a->name() == attr->name()) {
      throw ArraySchemaEvolutionException(
          "Cannot add attribute; Input attribute name is already there");
    }
  }

  // Create new attribute and potentially set a default name
  /*
   * At present, the container for attributes within the schema evolution
   * object is based on `unique_ptr`. The argument is `shared_ptr`, since
   * that's how C API handles and `class Array` store them. It might be better
   * to change the container type, but until then, we copy the attribute into
   * a new allocation.
   */
  attributes_to_add_.push_back(
      tdb_unique_ptr<Attribute>(tdb_new(Attribute, *attr)));
  if (attributes_to_drop_.find(attr->name()) != attributes_to_drop_.end()) {
    attributes_to_drop_.erase(attr->name());
  }
}

std::vector<std::string> ArraySchemaEvolution::attribute_names_to_add() const {
  std::lock_guard<std::mutex> lock(mtx_);
  std::vector<std::string> names;
  names.reserve(attributes_to_add_.size());
  for (auto& entry : attributes_to_add_) {
    names.emplace_back(entry->name());
  }
  return names;
}

const Attribute* ArraySchemaEvolution::attribute_to_add(
    const std::string& name) const {
  std::lock_guard<std::mutex> lock(mtx_);
  for (const auto& attr : attributes_to_add_) {
    if (attr->name() == name) {
      return attr.get();
    }
  }
  return nullptr;
}

void ArraySchemaEvolution::drop_attribute(const std::string& attribute_name) {
  std::lock_guard<std::mutex> lock(mtx_);
  attributes_to_drop_.insert(attribute_name);

  // Remove any added attributes with this name
  attributes_to_add_.erase(
      std::remove_if(
          attributes_to_add_.begin(),
          attributes_to_add_.end(),
          [&](auto& a) { return a->name() == attribute_name; }),
      attributes_to_add_.end());
}

std::vector<std::string> ArraySchemaEvolution::attribute_names_to_drop() const {
  std::lock_guard<std::mutex> lock(mtx_);
  std::vector<std::string> names;
  names.reserve(attributes_to_drop_.size());
  for (auto& name : attributes_to_drop_) {
    names.emplace_back(name);
  }
  return names;
}

void ArraySchemaEvolution::add_enumeration(shared_ptr<const Enumeration> enmr) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (enmr == nullptr) {
    throw ArraySchemaEvolutionException(
        "Cannot add enumeration; Input enumeration is null");
  }

  auto it = enumerations_to_add_map_.find(enmr->name());
  if (it != enumerations_to_add_map_.end()) {
    throw ArraySchemaEvolutionException(
        "Cannot add enumeration; Input enumeration name is already added");
  }

  enumerations_to_add_map_[enmr->name()] = enmr;
}

std::vector<std::string> ArraySchemaEvolution::enumeration_names_to_add()
    const {
  std::lock_guard<std::mutex> lock(mtx_);
  std::vector<std::string> names;
  names.reserve(enumerations_to_add_map_.size());
  for (auto elem : enumerations_to_add_map_) {
    names.push_back(elem.first);
  }

  return names;
}

shared_ptr<const Enumeration> ArraySchemaEvolution::enumeration_to_add(
    const std::string& name) const {
  std::lock_guard<std::mutex> lock(mtx_);
  auto it = enumerations_to_add_map_.find(name);

  if (it == enumerations_to_add_map_.end()) {
    return nullptr;
  }

  return it->second;
}

void ArraySchemaEvolution::extend_enumeration(
    shared_ptr<const Enumeration> enmr) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (enmr == nullptr) {
    throw ArraySchemaEvolutionException(
        "Cannot extend enumeration; Input enumeration is null");
  }

  auto it = enumerations_to_extend_map_.find(enmr->name());
  if (it != enumerations_to_extend_map_.end()) {
    throw ArraySchemaEvolutionException(
        "Cannot extend enumeration; Input enumeration name has already "
        "been extended in this evolution.");
  }

  enumerations_to_extend_map_[enmr->name()] = enmr;
}

std::vector<std::string> ArraySchemaEvolution::enumeration_names_to_extend()
    const {
  std::lock_guard<std::mutex> lock(mtx_);
  std::vector<std::string> names;
  names.reserve(enumerations_to_extend_map_.size());
  for (auto elem : enumerations_to_extend_map_) {
    names.push_back(elem.first);
  }

  return names;
}

shared_ptr<const Enumeration> ArraySchemaEvolution::enumeration_to_extend(
    const std::string& name) const {
  std::lock_guard<std::mutex> lock(mtx_);
  auto it = enumerations_to_extend_map_.find(name);

  if (it == enumerations_to_extend_map_.end()) {
    return nullptr;
  }

  return it->second;
}

void ArraySchemaEvolution::drop_enumeration(
    const std::string& enumeration_name) {
  std::lock_guard<std::mutex> lock(mtx_);
  enumerations_to_drop_.insert(enumeration_name);

  auto add_it = enumerations_to_add_map_.find(enumeration_name);
  if (add_it != enumerations_to_add_map_.end()) {
    // Reset the pointer and erase it
    enumerations_to_add_map_.erase(add_it);
  }

  auto extend_it = enumerations_to_extend_map_.find(enumeration_name);
  if (extend_it != enumerations_to_extend_map_.end()) {
    enumerations_to_extend_map_.erase(extend_it);
  }
}

std::vector<std::string> ArraySchemaEvolution::enumeration_names_to_drop()
    const {
  std::lock_guard<std::mutex> lock(mtx_);
  std::vector<std::string> names;
  names.reserve(enumerations_to_drop_.size());
  for (auto& name : enumerations_to_drop_) {
    names.emplace_back(name);
  }
  return names;
}

void ArraySchemaEvolution::set_timestamp_range(
    const std::pair<uint64_t, uint64_t>& timestamp_range) {
  if (timestamp_range.first != timestamp_range.second) {
    throw ArraySchemaEvolutionException(
        "Cannot set timestamp range; first element " +
        std::to_string(timestamp_range.first) + " and second element " +
        std::to_string(timestamp_range.second) + " are not equal!");
  }
  timestamp_range_ = timestamp_range;
}

std::pair<uint64_t, uint64_t> ArraySchemaEvolution::timestamp_range() const {
  return std::pair<uint64_t, uint64_t>(
      timestamp_range_.first, timestamp_range_.second);
}

void ArraySchemaEvolution::expand_current_domain(
    shared_ptr<CurrentDomain> current_domain) {
  if (current_domain == nullptr) {
    throw ArraySchemaEvolutionException(
        "Cannot expand the array current domain; Input current domain is "
        "null");
  }

  if (current_domain->empty()) {
    throw ArraySchemaEvolutionException(
        "Unable to expand the array current domain, the new current domain "
        "specified is empty");
  }

  std::lock_guard<std::mutex> lock(mtx_);
  current_domain_to_expand_ = current_domain;
}

shared_ptr<CurrentDomain> ArraySchemaEvolution::current_domain_to_expand()
    const {
  std::lock_guard<std::mutex> lock(mtx_);

  return current_domain_to_expand_;
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

void ArraySchemaEvolution::clear() {
  attributes_to_add_.clear();
  attributes_to_drop_.clear();
  enumerations_to_add_map_.clear();
  enumerations_to_drop_.clear();
  timestamp_range_ = {0, 0};
  current_domain_to_expand_ = nullptr;
}

}  // namespace tiledb::sm
