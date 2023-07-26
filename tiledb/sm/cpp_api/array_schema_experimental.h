/**
 * @file   array_schema_experimental.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file declares the experimental C++ API for the array schema.
 */

#ifndef TILEDB_CPP_API_ARRAY_SCHEMA_EXPERIMENTAL_H
#define TILEDB_CPP_API_ARRAY_SCHEMA_EXPERIMENTAL_H

#include "array_schema.h"
#include "dimension_label_experimental.h"
#include "enumeration_experimental.h"
#include "filter_list.h"
#include "tiledb_experimental.h"

#include <optional>

namespace tiledb {
class ArraySchemaExperimental {
 public:
  /**
   * Load an ArraySchema from the given URI with all of its enumerations.
   *
   * @param ctx The TileDB context.
   * @param uri The URI to load from.
   * @return ArraySchema The loaded array schema.
   */
  static ArraySchema load_with_enumerations(
      const Context& ctx, const std::string& uri) {
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    tiledb_array_schema_t* schema;
    ctx.handle_error(tiledb_array_schema_load_with_enumerations(
        c_ctx, uri.c_str(), &schema));
    return ArraySchema(ctx, schema);
  }

  /**
   * Adds a DimensionLabel to the array.
   *
   * @param ctx TileDB context.
   * @param array_schema Target array schema.
   * @param dim_index The index number of the dimension the labels will be
   *     applied to.
   * @param name The name of the dimension label.
   * @param label_order The order (increasing or decreasing) of the labels.
   * @param label_type The data type of the labels.
   * @param filter_list Filters to apply the label data.
   */
  static void add_dimension_label(
      const Context& ctx,
      ArraySchema& array_schema,
      uint32_t dim_index,
      const std::string& name,
      tiledb_data_order_t label_order,
      tiledb_datatype_t label_type,
      std::optional<FilterList> filter_list = std::nullopt) {
    ctx.handle_error(tiledb_array_schema_add_dimension_label(
        ctx.ptr().get(),
        array_schema.ptr().get(),
        dim_index,
        name.c_str(),
        label_order,
        label_type));
    if (filter_list.has_value()) {
      ctx.handle_error(tiledb_array_schema_set_dimension_label_filter_list(
          ctx.ptr().get(),
          array_schema.ptr().get(),
          name.c_str(),
          filter_list.value().ptr().get()));
    }
  }

  /**
   * Adds a DimensionLabel to the array schema.
   *
   * @tparam T Type of the dimension domain the labels are added to.
   * @param ctx TileDB context.
   * @param array_schema Target array schema.
   * @param dim_index The index number of the dimension the labels will be
   *     applied to.
   * @param name The name of the dimension label.
   * @param label_order The order (increasing or decreasing) of the labels.
   * @param label_type The data type of the labels.
   * @param dim_tile_extent Tile extent for the dimension of the dimension
   *     label.
   * @param filter_list Filters to apply the label data.
   */
  template <typename T>
  static void add_dimension_label(
      const Context& ctx,
      ArraySchema& array_schema,
      uint32_t dim_idx,
      const std::string& name,
      tiledb_data_order_t label_order,
      tiledb_datatype_t label_type,
      T dim_tile_extent,
      std::optional<FilterList> filter_list = std::nullopt) {
    using DataT = impl::TypeHandler<T>;
    static_assert(
        DataT::tiledb_num == 1,
        "Dimension label types cannot be compound, use an arithmetic type.");
    ArraySchemaExperimental::add_dimension_label(
        ctx, array_schema, dim_idx, name, label_order, label_type, filter_list);
    ctx.handle_error(tiledb_array_schema_set_dimension_label_tile_extent(
        ctx.ptr().get(),
        array_schema.ptr().get(),
        name.c_str(),
        DataT::tiledb_type,
        *dim_tile_extent));
  }

  /**
   * Checks if the schema has a dimension label of the given name.
   *
   * @param ctx TileDB context.
   * @param array_schema Target array schema.
   * @param name Name of the target dimension label to check for.
   */
  static bool has_dimension_label(
      const Context& ctx,
      const ArraySchema& array_schema,
      const std::string& name) {
    int32_t has_dim_label;
    ctx.handle_error(tiledb_array_schema_has_dimension_label(
        ctx.ptr().get(),
        array_schema.ptr().get(),
        name.c_str(),
        &has_dim_label));
    return has_dim_label != 0;
  }

  /**
   * Returns a dimension label from the array schema.
   *
   * @param ctx TileDB context.
   * @param array_schema Target array schema.
   * @param name Name of the target dimension label to return.
   */
  static DimensionLabel dimension_label(
      const Context& ctx,
      const ArraySchema& array_schema,
      const std::string& name) {
    tiledb_dimension_label_t* dim_label;
    ctx.handle_error(tiledb_array_schema_get_dimension_label_from_name(
        ctx.ptr().get(), array_schema.ptr().get(), name.c_str(), &dim_label));
    return DimensionLabel(ctx, dim_label);
  }

  /**
   * Add an enumeration to the array schema.
   *
   * @param ctx TileDB context.
   * @param array_schema Target array schema.
   * @param enmr The enumeration to add.
   */
  static void add_enumeration(
      const Context& ctx,
      const ArraySchema& array_schema,
      const Enumeration& enmr) {
    ctx.handle_error(tiledb_array_schema_add_enumeration(
        ctx.ptr().get(), array_schema.ptr().get(), enmr.ptr().get()));
  }
};

}  // namespace tiledb

#endif
