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
#include "tiledb/sm/array_schema/dimension_label_reference.h"
#include "tiledb/sm/c_api/api_exception_safety.h"
#include "tiledb/sm/c_api/experimental/api_exception_safety.h"
#include "tiledb/sm/c_api/experimental/tiledb_struct_def.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/group/group_v1.h"
#include "tiledb/sm/rest/rest_client.h"

using namespace tiledb::common;

namespace tiledb::common::detail {

int32_t tiledb_array_schema_add_dimension_label(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const uint32_t dim_id,
    const char* name,
    tiledb_dimension_label_schema_t* dim_label_schema) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array_schema) ||
      sanity_check(ctx, dim_label_schema)) {
    return TILEDB_ERR;
  }
  /** Note: The call to make_shared creates a copy of the array schemas and
   * the user-visible handles no longer refere to the same objects in the
   *array schema.
   **/
  if (SAVE_ERROR_CATCH(
          ctx,
          array_schema->array_schema_->add_dimension_label(
              dim_id,
              name,
              make_shared<tiledb::sm::DimensionLabelSchema>(
                  HERE(), dim_label_schema->dim_label_schema_.get())))) {
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

int32_t tiledb_array_schema_has_dimension_label(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char* name,
    int32_t* has_dim_label) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  bool is_dim_label = array_schema->array_schema_->is_dim_label(name);
  *has_dim_label = is_dim_label ? 1 : 0;
  return TILEDB_OK;
}

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
  if (sanity_check(ctx) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

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

int32_t tiledb_query_set_label_data_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    void* buffer,
    uint64_t* buffer_size) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;
  query->query_->set_label_data_buffer(name, buffer, buffer_size);
  return TILEDB_OK;
}

int32_t tiledb_query_set_label_offsets_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint64_t* buffer,
    uint64_t* buffer_size) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;
  query->query_->set_label_offsets_buffer(name, buffer, buffer_size);
  return TILEDB_OK;
}

int32_t tiledb_query_get_label_data_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    void** buffer,
    uint64_t** buffer_size) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;
  query->query_->get_label_data_buffer(name, buffer, buffer_size);
  return TILEDB_OK;
}

int32_t tiledb_query_get_label_offsets_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint64_t** buffer,
    uint64_t** buffer_size) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;
  query->query_->get_label_offsets_buffer(name, buffer, buffer_size);
  return TILEDB_OK;
}

int32_t tiledb_subarray_add_label_range(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* label_name,
    const void* start,
    const void* end,
    const void* stride) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, subarray) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  subarray->subarray_->add_label_range(label_name, start, end, stride);
  return TILEDB_OK;
}

int32_t tiledb_subarray_add_label_range_var(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* label_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, subarray) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  subarray->subarray_->add_label_range_var(
      label_name, start, start_size, end, end_size);
  return TILEDB_OK;
}

int32_t tiledb_subarray_get_label_range(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, subarray) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  subarray->subarray_->get_label_range(dim_name, range_idx, start, end, stride);
  return TILEDB_OK;
}

int32_t tiledb_subarray_get_label_range_num(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t* range_num) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, subarray) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  subarray->subarray_->get_label_range_num(dim_name, range_num);
  return TILEDB_OK;
}

int32_t tiledb_subarray_get_label_range_var(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    void* start,
    void* end) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, subarray) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  subarray->subarray_->get_label_range_var(dim_name, range_idx, start, end);
  return TILEDB_OK;
}

int32_t tiledb_subarray_get_label_range_var_size(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, subarray) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  subarray->subarray_->get_label_range_var_size(
      dim_name, range_idx, start_size, end_size);
  return TILEDB_OK;
}

}  // namespace tiledb::common::detail

int32_t tiledb_array_schema_add_dimension_label(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const uint32_t dim_id,
    const char* name,
    tiledb_dimension_label_schema_t* dim_label_schema) noexcept {
  return api_entry_context<detail::tiledb_array_schema_add_dimension_label>(
      ctx, array_schema, dim_id, name, dim_label_schema);
}

int32_t tiledb_array_schema_has_dimension_label(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char* name,
    int32_t* has_dim_label) noexcept {
  return api_entry_context<detail::tiledb_array_schema_has_dimension_label>(
      ctx, array_schema, name, has_dim_label);
}

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
  return api_entry_context<detail::tiledb_dimension_label_schema_alloc>(
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
  return api_entry_void<detail::tiledb_dimension_label_schema_free>(
      dim_label_schema);
}

int32_t tiledb_query_set_label_data_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    void* buffer,
    uint64_t* buffer_size) noexcept {
  return api_entry_context<detail::tiledb_query_set_label_data_buffer>(
      ctx, query, name, buffer, buffer_size);
}

int32_t tiledb_query_set_label_offsets_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint64_t* buffer,
    uint64_t* buffer_size) noexcept {
  return api_entry_context<detail::tiledb_query_set_label_offsets_buffer>(
      ctx, query, name, buffer, buffer_size);
}

int32_t tiledb_query_get_label_data_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    void** buffer,
    uint64_t** buffer_size) noexcept {
  return api_entry_context<detail::tiledb_query_get_label_data_buffer>(
      ctx, query, name, buffer, buffer_size);
}

int32_t tiledb_query_get_label_offsets_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint64_t** buffer,
    uint64_t** buffer_size) noexcept {
  return api_entry_context<detail::tiledb_query_get_label_offsets_buffer>(
      ctx, query, name, buffer, buffer_size);
}

int32_t tiledb_subarray_add_label_range(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* label_name,
    const void* start,
    const void* end,
    const void* stride) noexcept {
  return api_entry<detail::tiledb_subarray_add_label_range>(
      ctx, subarray, label_name, start, end, stride);
}

int32_t tiledb_subarray_add_label_range_var(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* label_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) noexcept {
  return api_entry<detail::tiledb_subarray_add_label_range_var>(
      ctx, subarray, label_name, start, start_size, end, end_size);
}

int32_t tiledb_subarray_get_label_range(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) noexcept {
  return api_entry<detail::tiledb_subarray_get_label_range>(
      ctx, subarray, dim_name, range_idx, start, end, stride);
}

int32_t tiledb_subarray_get_label_range_num(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t* range_num) noexcept {
  return api_entry<detail::tiledb_subarray_get_label_range_num>(
      ctx, subarray, dim_name, range_num);
}

int32_t tiledb_subarray_get_label_range_var(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    void* start,
    void* end) noexcept {
  return api_entry<detail::tiledb_subarray_get_label_range_var>(
      ctx, subarray, dim_name, range_idx, start, end);
}

int32_t tiledb_subarray_get_label_range_var_size(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) noexcept {
  return api_entry<detail::tiledb_subarray_get_label_range_var_size>(
      ctx, subarray, dim_name, range_idx, start_size, end_size);
}
