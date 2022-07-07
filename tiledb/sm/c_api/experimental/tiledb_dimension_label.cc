/**
 * @file tiledb/sm/c_api/tiledb_dimension_label.cc
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
 */

#include "tiledb/sm/c_api/experimental/tiledb_dimension_label.h"
#include "tiledb/sm/c_api/api_exception_safety.h"
#include "tiledb/sm/c_api/experimental/api_exception_safety.h"
#include "tiledb/sm/c_api/experimental/tiledb_struct_def.h"
#include "tiledb/sm/c_api/tiledb.h"

using namespace tiledb::common;

namespace tiledb::common::detail {

int32_t tiledb_dimension_label_schema_alloc(
    tiledb_ctx_t* ctx,
    tiledb_label_order_t label_order,
    tiledb_datatype_t index_type,
    const void* index_domain,
    const void* index_tile_extent,
    tiledb_datatype_t label_type,
    const void* label_domain,
    const void* label_tile_extent,
    tiledb_dimension_label_schema_t** dim_label_schema) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a dimension label schema struct
  *dim_label_schema = new (std::nothrow) tiledb_dimension_label_schema_t;
  if (*dim_label_schema == nullptr) {
    auto st =
        Status_Error("Failed to allocate TileDB dimension label schema object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new DimensionLabelSchema object
  (*dim_label_schema)->dim_label_schema_ =
      make_shared<tiledb::sm::DimensionLabelSchema>(
          HERE(),
          static_cast<tiledb::sm::LabelOrder>(label_order),
          static_cast<tiledb::sm::Datatype>(index_type),
          index_domain,
          index_tile_extent,
          static_cast<tiledb::sm::Datatype>(label_type),
          label_domain,
          label_tile_extent);
  if ((*dim_label_schema)->dim_label_schema_ == nullptr) {
    delete *dim_label_schema;
    *dim_label_schema = nullptr;
    auto st =
        Status_Error("Failed to allocate TileDB dimension label schema object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_dimension_label_schema_free(
    tiledb_dimension_label_schema_t** dim_label_schema) {
  if (dim_label_schema != nullptr && *dim_label_schema != nullptr) {
    delete *dim_label_schema;
    *dim_label_schema = nullptr;
  }
}

}  // namespace tiledb::common::detail

int32_t tiledb_dimension_label_schema_alloc(
    tiledb_ctx_t* ctx,
    tiledb_label_order_t label_order,
    tiledb_datatype_t index_type,
    const void* index_domain,
    const void* index_tile_extent,
    tiledb_datatype_t label_type,
    const void* label_domain,
    const void* label_tile_extent,
    tiledb_dimension_label_schema_t** dim_label_schema) noexcept {
  return api_entry<detail::tiledb_dimension_label_schema_alloc>(
      ctx,
      label_order,
      index_type,
      index_domain,
      index_tile_extent,
      label_type,
      label_domain,
      label_tile_extent,
      dim_label_schema);
}

void tiledb_dimension_label_schema_free(
    tiledb_dimension_label_schema_t** dim_label_schema) noexcept {
  return api_entry<detail::tiledb_dimension_label_schema_free>(
      dim_label_schema);
}
