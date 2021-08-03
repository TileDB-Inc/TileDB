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

Status ArraySchemaEvolution::evolve_schema(
    const ArraySchema* orig_schema, ArraySchema** new_schema) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (orig_schema == nullptr) {
    return LOG_STATUS(Status::ArraySchemaEvolutionError(
        "Cannot evolve schema; Input array schema is null"));
  }

  auto schema = tdb_new(ArraySchema, orig_schema);

  // Add attributes.
  for (auto& attr : attributes_to_add_map_) {
    RETURN_NOT_OK(schema->add_attribute(attr.second, false));
  }

  // Drop attributes.
  for (auto& attr_name : attributes_to_drop_) {
    bool has_attr = false;
    RETURN_NOT_OK(schema->has_attribute(attr_name, &has_attr));
    if (has_attr) {
      // TODO in another branch
      // RETURN_NOT_OK(schema->drop_attribute(attr_name));
    }
  }

  // Do other things

  *new_schema = schema;
  return Status::Ok();
}

Status ArraySchemaEvolution::add_attribute(const Attribute* attr) {
  std::lock_guard<std::mutex> lock(mtx_);
  // Sanity check
  if (attr == nullptr)
    return LOG_STATUS(Status::ArraySchemaEvolutionError(
        "Cannot add attribute; Input attribute is null"));

  // Create new attribute and potentially set a default name
  auto new_attr = tdb_new(Attribute, attr);
  attributes_to_add_map_[new_attr->name()] = new_attr;

  return Status::Ok();
}

Status ArraySchemaEvolution::drop_attribute(const std::string& attribute_name) {
  std::lock_guard<std::mutex> lock(mtx_);
  attributes_to_drop_.insert(attribute_name);
  return Status::Ok();
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

void ArraySchemaEvolution::clear() {
  for (auto& attr : attributes_to_add_map_) {
    tdb_delete(attr.second);
  }
  attributes_to_add_map_.clear();

  attributes_to_drop_.clear();
}

}  // namespace sm
}  // namespace tiledb
