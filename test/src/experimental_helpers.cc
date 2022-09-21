/**
 * @file experimental_helpers.cc
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
 */

#include "experimental_helpers.h"

namespace tiledb::test {

void add_dimension_label(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const std::string& label_name,
    const uint32_t dim_idx,
    tiledb_label_order_t label_order,
    tiledb_datatype_t label_datatype,
    const void* label_domain,
    const void* label_tile_extent,
    const void* index_tile_extent,
    optional<uint32_t> label_cell_val_num,
    optional<std::pair<tiledb_filter_type_t, int>> label_filters,
    optional<std::pair<tiledb_filter_type_t, int>> index_filters,
    optional<uint64_t> capacity,
    optional<bool> allows_dups) {
  // Get definition of the dimension the label is being added to.
  const auto* dim = array_schema->array_schema_->dimension_ptr(dim_idx);
  auto dim_type = dim->type();
  const auto& dim_domain = dim->domain();

  // Create the dimension label.
  tiledb_dimension_label_schema_t* dim_label_schema;
  REQUIRE_TILEDB_OK(tiledb_dimension_label_schema_alloc(
      ctx,
      label_order,
      static_cast<tiledb_datatype_t>(dim_type),
      dim_domain.data(),
      index_tile_extent,
      label_datatype,
      label_domain,
      label_tile_extent,
      &dim_label_schema));

  // Update the proprerties of the dimension label.
  UNSCOPED_INFO("API for adding label cell value number not yet implemented.");
  REQUIRE(!label_cell_val_num.has_value());
  UNSCOPED_INFO("API for adding label filters not yet implemented.");
  REQUIRE(!label_filters.has_value());
  UNSCOPED_INFO("API for adding index filters not yet implemented.");
  REQUIRE(!index_filters.has_value());
  UNSCOPED_INFO("API for adding capacity not yet implemented.");
  REQUIRE(!capacity.has_value());
  UNSCOPED_INFO("API for allowing duplicates not yet implemented.");
  REQUIRE(!allows_dups.has_value());

  // Add the dimension label to the array schema.
  REQUIRE_TILEDB_OK(tiledb_array_schema_add_dimension_label(
      ctx, array_schema, dim_idx, label_name.c_str(), dim_label_schema));
  tiledb_dimension_label_schema_free(&dim_label_schema);
}

}  // namespace tiledb::test
