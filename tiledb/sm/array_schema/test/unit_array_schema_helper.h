/**
 * @file tiledb/sm/array_schema/test/unit_array_schema_helper.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Helper functions for array schema unit tests
 */

#ifndef TILEDB_ARRAY_SCHEMA_HELPER_H
#define TILEDB_ARRAY_SCHEMA_HELPER_H

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/dimension_label_schema.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/data_order.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/filter/filter_pipeline.h"

using namespace tiledb::common;
namespace tiledb::sm::test {

/** Helper function for creating a dimension */
template <typename T>
shared_ptr<Dimension> make_dimension(
    const std::string& name,
    Datatype type,
    const uint32_t cell_val_num,
    const T domain_start,
    const T domain_stop,
    const T tile_data) {
  T domain_data[2]{domain_start, domain_stop};
  Range domain(&domain_data[0], 2 * sizeof(T));
  ByteVecValue tile{};
  tile.assign_as<T>(tile_data);
  return make_shared<Dimension>(
      HERE(), name, type, cell_val_num, domain, FilterPipeline(), tile);
}

/** Helper function for creating an attribute */
template <typename T>
shared_ptr<Attribute> make_attribute(
    const std::string& name,
    Datatype type,
    const bool nullable,
    const uint32_t cell_val_num,
    const T fill_data) {
  ByteVecValue fill;
  fill.assign_as<T>(fill_data);
  return make_shared<Attribute>(
      HERE(), name, type, nullable, cell_val_num, FilterPipeline(), fill, 0);
}

/** Helper function for creating an array schema */
inline shared_ptr<ArraySchema> make_array_schema(
    const ArrayType array_type,
    const std::vector<shared_ptr<Dimension>>& dims,
    const std::vector<shared_ptr<Attribute>>& attrs,
    const Layout cell_layout = Layout::ROW_MAJOR,
    const Layout tile_layout = Layout::ROW_MAJOR) {
  auto array_schema = make_shared<ArraySchema>(HERE(), array_type);
  auto status = array_schema->set_domain(
      make_shared<Domain>(cell_layout, dims, tile_layout));
  CHECK(status.ok());
  for (const auto& attr : attrs) {
    CHECK(array_schema->add_attribute(attr).ok());
  }
  return array_schema;
}

}  // namespace tiledb::sm::test

#endif
