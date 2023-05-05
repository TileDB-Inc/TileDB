/**
 * @file   array_schema_evolution.cc
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
 * This file implements the ArraySchemaEvolution class.
 */

#include "tiledb/sm/array_schema/array_schema_evolution.h"
#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
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

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArraySchemaEvolution::ArraySchemaEvolution() {
}

ArraySchemaEvolution::~ArraySchemaEvolution() {
  clear();
}

/* ****************************** */
/*               API              */
/* ****************************** */

tuple<Status, optional<shared_ptr<ArraySchema>>>
ArraySchemaEvolution::evolve_schema(
    const shared_ptr<const ArraySchema>& orig_schema) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (orig_schema == nullptr) {
    return {
        LOG_STATUS(Status_ArraySchemaEvolutionError(
            "Cannot evolve schema; Input array schema is null")),
        nullopt};
  }

  auto schema = make_shared<ArraySchema>(HERE(), *(orig_schema.get()));

  // Add attributes.
  for (auto& attr : attributes_to_add_map_) {
    RETURN_NOT_OK_TUPLE(schema->add_attribute(attr.second, false), nullopt);
  }

  // Drop attributes.
  for (auto& attr_name : attributes_to_drop_) {
    bool has_attr = false;
    RETURN_NOT_OK_TUPLE(schema->has_attribute(attr_name, &has_attr), nullopt);
    if (has_attr) {
      RETURN_NOT_OK_TUPLE(schema->drop_attribute(attr_name), nullopt);
    }
  }

  // Do other things

  // Set timestamp, if specified
  if (std::get<0>(timestamp_range_) != 0) {
    RETURN_NOT_OK_TUPLE(
        schema.get()->set_timestamp_range(timestamp_range_), nullopt);
    schema->generate_uri(timestamp_range_);
  } else {
    // Generate new schema URI
    schema->generate_uri();
  }

  return {Status::Ok(), schema};
}

Status ArraySchemaEvolution::add_attribute(const Attribute* attr) {
  std::lock_guard<std::mutex> lock(mtx_);
  // Sanity check
  if (attr == nullptr)
    return LOG_STATUS(Status_ArraySchemaEvolutionError(
        "Cannot add attribute; Input attribute is null"));

  if (attributes_to_add_map_.find(attr->name()) !=
      attributes_to_add_map_.end()) {
    return LOG_STATUS(Status_ArraySchemaEvolutionError(
        "Cannot add attribute; Input attribute name is already there"));
  }

  // Create new attribute and potentially set a default name
  attributes_to_add_map_[attr->name()] =
      tdb_unique_ptr<Attribute>(tdb_new(Attribute, attr));
  if (attributes_to_drop_.find(attr->name()) != attributes_to_drop_.end()) {
    attributes_to_drop_.erase(attr->name());
  }

  return Status::Ok();
}

std::vector<std::string> ArraySchemaEvolution::attribute_names_to_add() const {
  std::lock_guard<std::mutex> lock(mtx_);
  std::vector<std::string> names;
  names.reserve(attributes_to_add_map_.size());
  for (auto it = attributes_to_add_map_.begin();
       it != attributes_to_add_map_.end();
       ++it) {
    names.push_back(it->first);
  }
  return names;
}

const Attribute* ArraySchemaEvolution::attribute_to_add(
    const std::string& name) const {
  std::lock_guard<std::mutex> lock(mtx_);
  auto it = attributes_to_add_map_.find(name);
  return (it == attributes_to_add_map_.end()) ? nullptr : it->second.get();
}

Status ArraySchemaEvolution::drop_attribute(const std::string& attribute_name) {
  std::lock_guard<std::mutex> lock(mtx_);
  attributes_to_drop_.insert(attribute_name);
  if (attributes_to_add_map_.find(attribute_name) !=
      attributes_to_add_map_.end()) {
    // Reset the pointer and erase it
    attributes_to_add_map_.erase(attribute_name);
  }
  return Status::Ok();
}

std::vector<std::string> ArraySchemaEvolution::attribute_names_to_drop() const {
  std::lock_guard<std::mutex> lock(mtx_);
  std::vector<std::string> names;
  names.reserve(attributes_to_drop_.size());
  for (auto it = attributes_to_drop_.begin(); it != attributes_to_drop_.end();
       ++it) {
    names.push_back(*it);
  }
  return names;
}

Status ArraySchemaEvolution::set_timestamp_range(
    const std::pair<uint64_t, uint64_t>& timestamp_range) {
  if (timestamp_range.first != timestamp_range.second) {
    throw std::runtime_error(std::string(
        "Cannot set timestamp range; first element " +
        std::to_string(timestamp_range.first) + " and second element " +
        std::to_string(timestamp_range.second) + " are not equal!"));
  }
  timestamp_range_ = timestamp_range;
  return Status::Ok();
}

std::pair<uint64_t, uint64_t> ArraySchemaEvolution::timestamp_range() const {
  return std::pair<uint64_t, uint64_t>(
      timestamp_range_.first, timestamp_range_.second);
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

void ArraySchemaEvolution::clear() {
  attributes_to_add_map_.clear();
  attributes_to_drop_.clear();
  timestamp_range_ = {0, 0};
}

}  // namespace sm
}  // namespace tiledb
