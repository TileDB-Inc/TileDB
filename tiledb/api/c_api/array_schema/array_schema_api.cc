/**
 * @file tiledb/api/c_api/array_schema/array_schema_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file defines C API functions for the array schema section.
 */

#include "array_schema_api_deprecated.h"
#include "array_schema_api_experimental.h"
#include "array_schema_api_internal.h"

#include "tiledb/api/c_api/attribute/attribute_api_external_experimental.h"
#include "tiledb/api/c_api/attribute/attribute_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api/current_domain/current_domain_api_external_experimental.h"
#include "tiledb/api/c_api/current_domain/current_domain_api_internal.h"
#include "tiledb/api/c_api/domain/domain_api_internal.h"
#include "tiledb/api/c_api/enumeration/enumeration_api_internal.h"
#include "tiledb/api/c_api/filter_list/filter_list_api_internal.h"
#include "tiledb/api/c_api/string/string_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/common/memory_tracker.h"

namespace tiledb::api {

capi_return_t tiledb_array_type_to_str(
    tiledb_array_type_t array_type, const char** str) {
  const auto& strval =
      tiledb::sm::array_type_str((tiledb::sm::ArrayType)array_type);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

capi_return_t tiledb_array_type_from_str(
    const char* str, tiledb_array_type_t* array_type) {
  tiledb::sm::ArrayType val = tiledb::sm::ArrayType::DENSE;
  if (!tiledb::sm::array_type_enum(str, &val).ok()) {
    return TILEDB_ERR;
  }
  *array_type = (tiledb_array_type_t)val;
  return TILEDB_OK;
}

capi_return_t tiledb_layout_to_str(tiledb_layout_t layout, const char** str) {
  const auto& strval = tiledb::sm::layout_str((tiledb::sm::Layout)layout);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

capi_return_t tiledb_layout_from_str(const char* str, tiledb_layout_t* layout) {
  tiledb::sm::Layout val = tiledb::sm::Layout::ROW_MAJOR;
  if (!tiledb::sm::layout_enum(str, &val).ok()) {
    return TILEDB_ERR;
  }
  *layout = (tiledb_layout_t)val;
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_alloc(
    tiledb_ctx_t* ctx,
    tiledb_array_type_t array_type,
    tiledb_array_schema_t** array_schema) {
  ensure_output_pointer_is_valid(array_schema);

  // Create ArraySchema object
  auto memory_tracker = ctx->resources().create_memory_tracker();
  memory_tracker->set_type(tiledb::sm::MemoryTrackerType::ARRAY_CREATE);
  *array_schema = tiledb_array_schema_t::make_handle(
      static_cast<tiledb::sm::ArrayType>(array_type), memory_tracker);

  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_alloc_at_timestamp(
    tiledb_ctx_t* ctx,
    tiledb_array_type_t array_type,
    uint64_t t1,
    uint64_t t2,
    tiledb_array_schema_t** array_schema) {
  ensure_output_pointer_is_valid(array_schema);

  // Create ArraySchema object
  auto memory_tracker = ctx->resources().create_memory_tracker();
  memory_tracker->set_type(tiledb::sm::MemoryTrackerType::ARRAY_CREATE);
  *array_schema = tiledb_array_schema_t::make_handle(
      static_cast<tiledb::sm::ArrayType>(array_type),
      memory_tracker,
      std::make_pair(t1, t2));

  return TILEDB_OK;
}

void tiledb_array_schema_free(tiledb_array_schema_t** array_schema) {
  ensure_output_pointer_is_valid(array_schema);
  ensure_array_schema_is_valid(*array_schema);
  tiledb_array_schema_t::break_handle(*array_schema);
}

capi_return_t tiledb_array_schema_add_attribute(
    tiledb_array_schema_t* array_schema, tiledb_attribute_t* attr) {
  ensure_array_schema_is_valid(array_schema);
  ensure_attribute_is_valid(attr);

  /** Note: The call to make_shared creates a copy of the attribute and
   * the user-visible handle to the attr no longer refers to the same object
   * that's in the array_schema.
   **/
  throw_if_not_ok(array_schema->add_attribute(attr->copy_attribute()));
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_set_allows_dups(
    tiledb_array_schema_t* array_schema, int allows_dups) {
  ensure_array_schema_is_valid(array_schema);
  throw_if_not_ok(array_schema->set_allows_dups(allows_dups));
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_allows_dups(
    tiledb_array_schema_t* array_schema, int* allows_dups) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(allows_dups);
  *allows_dups = (int)array_schema->allows_dups();
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_version(
    tiledb_array_schema_t* array_schema, uint32_t* version) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(version);
  *version = (uint32_t)array_schema->version();
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_set_domain(
    tiledb_array_schema_t* array_schema, tiledb_domain_t* domain) {
  ensure_array_schema_is_valid(array_schema);
  ensure_domain_is_valid(domain);
  throw_if_not_ok(array_schema->set_domain(domain->copy_domain()));
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_set_capacity(
    tiledb_array_schema_t* array_schema, uint64_t capacity) {
  ensure_array_schema_is_valid(array_schema);
  array_schema->set_capacity(capacity);
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_set_cell_order(
    tiledb_array_schema_t* array_schema, tiledb_layout_t cell_order) {
  ensure_array_schema_is_valid(array_schema);
  throw_if_not_ok(array_schema->set_cell_order(
      static_cast<tiledb::sm::Layout>(cell_order)));
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_set_tile_order(
    tiledb_array_schema_t* array_schema, tiledb_layout_t tile_order) {
  ensure_array_schema_is_valid(array_schema);
  throw_if_not_ok(array_schema->set_tile_order(
      static_cast<tiledb::sm::Layout>(tile_order)));
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_timestamp_range(
    tiledb_array_schema_t* array_schema, uint64_t* lo, uint64_t* hi) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(lo);
  ensure_output_pointer_is_valid(hi);

  auto timestamp_range = array_schema->timestamp_range();
  *lo = std::get<0>(timestamp_range);
  *hi = std::get<1>(timestamp_range);

  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_enumeration_from_name(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const char* enumeration_name,
    tiledb_enumeration_t** enumeration) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(enumeration);

  if (enumeration_name == nullptr) {
    throw CAPIException("'enumeration_name' must not be null");
  }

  array_schema->load_enumeration(ctx, enumeration_name);

  auto ptr = array_schema->get_enumeration(enumeration_name);
  *enumeration = tiledb_enumeration_handle_t::make_handle(ptr);

  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_enumeration_from_attribute_name(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const char* attribute_name,
    tiledb_enumeration_t** enumeration) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(enumeration);

  if (attribute_name == nullptr) {
    throw CAPIException("'attribute_name' must not be null");
  }
  std::string attribute_name_string(attribute_name);
  auto found_attr = array_schema->shared_attribute(attribute_name_string);
  if (!found_attr) {
    throw CAPIException(
        std::string("Attribute name: ") +
        (attribute_name_string.empty() ? "<anonymous>" : attribute_name) +
        " does not exist for array " + array_schema->array_uri().to_string());
  }

  auto enumeration_name = found_attr->get_enumeration_name();
  if (!enumeration_name.has_value()) {
    *enumeration = nullptr;
    return TILEDB_OK;
  }

  array_schema->load_enumeration(ctx, enumeration_name->c_str());

  auto ptr = array_schema->get_enumeration(enumeration_name->c_str());
  *enumeration = tiledb_enumeration_handle_t::make_handle(ptr);

  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_add_enumeration(
    tiledb_array_schema_t* array_schema, tiledb_enumeration_t* enumeration) {
  ensure_array_schema_is_valid(array_schema);
  ensure_enumeration_is_valid(enumeration);
  array_schema->add_enumeration(enumeration->copy());
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_set_coords_filter_list(
    tiledb_array_schema_t* array_schema, tiledb_filter_list_t* filter_list) {
  ensure_array_schema_is_valid(array_schema);
  ensure_filter_list_is_valid(filter_list);
  throw_if_not_ok(
      array_schema->set_coords_filter_pipeline(filter_list->pipeline()));
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_set_offsets_filter_list(
    tiledb_array_schema_t* array_schema, tiledb_filter_list_t* filter_list) {
  ensure_array_schema_is_valid(array_schema);
  ensure_filter_list_is_valid(filter_list);
  throw_if_not_ok(array_schema->set_cell_var_offsets_filter_pipeline(
      filter_list->pipeline()));
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_set_validity_filter_list(
    tiledb_array_schema_t* array_schema, tiledb_filter_list_t* filter_list) {
  ensure_array_schema_is_valid(array_schema);
  ensure_filter_list_is_valid(filter_list);
  throw_if_not_ok(
      array_schema->set_cell_validity_filter_pipeline(filter_list->pipeline()));
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_check(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema) {
  ensure_array_schema_is_valid(array_schema);
  array_schema->check(ctx->resources().config());
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_array_type(
    const tiledb_array_schema_t* array_schema,
    tiledb_array_type_t* array_type) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(array_type);
  *array_type = static_cast<tiledb_array_type_t>(array_schema->array_type());
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_capacity(
    const tiledb_array_schema_t* array_schema, uint64_t* capacity) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(capacity);
  *capacity = array_schema->capacity();
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_cell_order(
    const tiledb_array_schema_t* array_schema, tiledb_layout_t* cell_order) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(cell_order);
  *cell_order = static_cast<tiledb_layout_t>(array_schema->cell_order());
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_coords_filter_list(
    tiledb_array_schema_t* array_schema, tiledb_filter_list_t** filter_list) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(filter_list);
  // Copy-construct a separate FilterPipeline object
  *filter_list = tiledb_filter_list_t::make_handle(
      sm::FilterPipeline{array_schema->coords_filters()});
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_offsets_filter_list(
    tiledb_array_schema_t* array_schema, tiledb_filter_list_t** filter_list) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(filter_list);
  // Copy-construct a separate FilterPipeline object
  *filter_list = tiledb_filter_list_t::make_handle(
      sm::FilterPipeline{array_schema->cell_var_offsets_filters()});
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_validity_filter_list(
    tiledb_array_schema_t* array_schema, tiledb_filter_list_t** filter_list) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(filter_list);
  // Copy-construct a separate FilterPipeline object
  *filter_list = tiledb_filter_list_t::make_handle(
      sm::FilterPipeline{array_schema->cell_validity_filters()});
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_domain(
    const tiledb_array_schema_t* array_schema, tiledb_domain_t** domain) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(domain);
  *domain = tiledb_domain_handle_t::make_handle(array_schema->shared_domain());
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_tile_order(
    const tiledb_array_schema_t* array_schema, tiledb_layout_t* tile_order) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(tile_order);
  *tile_order = static_cast<tiledb_layout_t>(array_schema->tile_order());
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_attribute_num(
    const tiledb_array_schema_t* array_schema, uint32_t* attribute_num) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(attribute_num);
  *attribute_num = array_schema->attribute_num();
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_dump(
    const tiledb_array_schema_t* array_schema, FILE* out) {
  // Note: this API is deprecated.
  ensure_array_schema_is_valid(array_schema);
  ensure_cstream_handle_is_valid(out);

  std::stringstream ss;
  ss << *array_schema->array_schema();
  size_t r = fwrite(ss.str().c_str(), sizeof(char), ss.str().size(), out);
  if (r != ss.str().size()) {
    throw CAPIException(
        "Error writing array schema " + array_schema->array_uri().to_string() +
        " to file");
  }
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_dump_str(
    const tiledb_array_schema_t* array_schema, tiledb_string_t** out) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(out);

  std::stringstream ss;
  ss << *array_schema->array_schema();
  *out = tiledb_string_handle_t::make_handle(ss.str());
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_attribute_from_index(
    const tiledb_array_schema_t* array_schema,
    uint32_t index,
    tiledb_attribute_t** attr) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(attr);

  uint32_t attribute_num = array_schema->attribute_num();
  if (attribute_num == 0) {
    *attr = nullptr;
    return TILEDB_OK;
  }
  if (index >= attribute_num) {
    std::ostringstream errmsg;
    errmsg << "Attribute index: " << index << " out of bounds given "
           << attribute_num << " attributes in array "
           << array_schema->array_uri().to_string();
    throw CAPIException(errmsg.str());
  }

  auto found_attr = array_schema->shared_attribute(index);
  if (!found_attr) {
    throw CAPIException("Attribute not found, but index is valid!");
  }
  *attr = tiledb_attribute_handle_t::make_handle(found_attr);
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_attribute_from_name(
    const tiledb_array_schema_t* array_schema,
    const char* name,
    tiledb_attribute_t** attr) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(attr);

  if (name == nullptr) {
    throw CAPIException("'attribute_name' must not be null");
  }

  uint32_t attribute_num = array_schema->attribute_num();
  if (attribute_num == 0) {
    *attr = nullptr;
    return TILEDB_OK;
  }
  std::string name_string(name);
  auto found_attr = array_schema->shared_attribute(name_string);
  if (!found_attr) {
    throw CAPIException(
        std::string("Attribute name: ") +
        (name_string.empty() ? "<anonymous>" : name) +
        " does not exist for array " + array_schema->array_uri().to_string());
  }
  *attr = tiledb_attribute_handle_t::make_handle(found_attr);
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_has_attribute(
    const tiledb_array_schema_t* array_schema,
    const char* name,
    int32_t* has_attr) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(has_attr);

  bool b;
  throw_if_not_ok(array_schema->has_attribute(name, &b));
  *has_attr = b ? 1 : 0;

  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_set_current_domain(
    tiledb_array_schema_t* array_schema,
    tiledb_current_domain_t* current_domain) {
  ensure_array_schema_is_valid(array_schema);
  ensure_handle_is_valid(current_domain);
  array_schema->set_current_domain(current_domain->current_domain());
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_current_domain(
    tiledb_array_schema_t* array_schema,
    tiledb_current_domain_t** current_domain) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(current_domain);

  // There is always a current domain on an ArraySchema instance,
  // when none was set explicitly, there is an empty current domain.
  *current_domain = tiledb_current_domain_handle_t::make_handle(
      array_schema->get_current_domain());
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_plain;
using tiledb::api::api_entry_void;
using tiledb::api::api_entry_with_context;

CAPI_INTERFACE(
    array_type_to_str, tiledb_array_type_t array_type, const char** str) {
  return api_entry_plain<tiledb::api::tiledb_array_type_to_str>(
      array_type, str);
}

CAPI_INTERFACE(
    array_type_from_str, const char* str, tiledb_array_type_t* array_type) {
  return api_entry_plain<tiledb::api::tiledb_array_type_from_str>(
      str, array_type);
}

CAPI_INTERFACE(layout_to_str, tiledb_layout_t layout, const char** str) {
  return api_entry_plain<tiledb::api::tiledb_layout_to_str>(layout, str);
}

CAPI_INTERFACE(layout_from_str, const char* str, tiledb_layout_t* layout) {
  return api_entry_plain<tiledb::api::tiledb_layout_from_str>(str, layout);
}

CAPI_INTERFACE(
    array_schema_alloc,
    tiledb_ctx_t* ctx,
    tiledb_array_type_t array_type,
    tiledb_array_schema_t** array_schema) {
  return api_entry_with_context<tiledb::api::tiledb_array_schema_alloc>(
      ctx, array_type, array_schema);
}

CAPI_INTERFACE(
    array_schema_alloc_at_timestamp,
    tiledb_ctx_t* ctx,
    tiledb_array_type_t array_type,
    uint64_t t1,
    uint64_t t2,
    tiledb_array_schema_t** array_schema) {
  return api_entry_with_context<
      tiledb::api::tiledb_array_schema_alloc_at_timestamp>(
      ctx, array_type, t1, t2, array_schema);
}

CAPI_INTERFACE_VOID(array_schema_free, tiledb_array_schema_t** array_schema) {
  return api_entry_void<tiledb::api::tiledb_array_schema_free>(array_schema);
}

CAPI_INTERFACE(
    array_schema_add_attribute,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_attribute_t* attr) {
  return api_entry_context<tiledb::api::tiledb_array_schema_add_attribute>(
      ctx, array_schema, attr);
}

CAPI_INTERFACE(
    array_schema_set_allows_dups,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    int allows_dups) {
  return api_entry_context<tiledb::api::tiledb_array_schema_set_allows_dups>(
      ctx, array_schema, allows_dups);
}

CAPI_INTERFACE(
    array_schema_get_allows_dups,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    int* allows_dups) {
  return api_entry_context<tiledb::api::tiledb_array_schema_get_allows_dups>(
      ctx, array_schema, allows_dups);
}

CAPI_INTERFACE(
    array_schema_get_version,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    uint32_t* version) {
  return api_entry_context<tiledb::api::tiledb_array_schema_get_version>(
      ctx, array_schema, version);
}

CAPI_INTERFACE(
    array_schema_set_domain,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_domain_t* domain) {
  return api_entry_context<tiledb::api::tiledb_array_schema_set_domain>(
      ctx, array_schema, domain);
}

CAPI_INTERFACE(
    array_schema_set_capacity,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    uint64_t capacity) {
  return api_entry_context<tiledb::api::tiledb_array_schema_set_capacity>(
      ctx, array_schema, capacity);
}

CAPI_INTERFACE(
    array_schema_set_cell_order,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_layout_t cell_order) {
  return api_entry_context<tiledb::api::tiledb_array_schema_set_cell_order>(
      ctx, array_schema, cell_order);
}

CAPI_INTERFACE(
    array_schema_set_tile_order,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_layout_t tile_order) {
  return api_entry_context<tiledb::api::tiledb_array_schema_set_tile_order>(
      ctx, array_schema, tile_order);
}

CAPI_INTERFACE(
    array_schema_timestamp_range,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    uint64_t* lo,
    uint64_t* hi) {
  return api_entry_context<tiledb::api::tiledb_array_schema_timestamp_range>(
      ctx, array_schema, lo, hi);
}

CAPI_INTERFACE(
    array_schema_get_enumeration_from_name,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const char* enumeration_name,
    tiledb_enumeration_t** enumeration) {
  return api_entry_with_context<
      tiledb::api::tiledb_array_schema_get_enumeration_from_name>(
      ctx, array_schema, enumeration_name, enumeration);
}

CAPI_INTERFACE(
    array_schema_get_enumeration_from_attribute_name,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const char* attribute_name,
    tiledb_enumeration_t** enumeration) {
  return api_entry_with_context<
      tiledb::api::tiledb_array_schema_get_enumeration_from_attribute_name>(
      ctx, array_schema, attribute_name, enumeration);
}

CAPI_INTERFACE(
    array_schema_add_enumeration,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_enumeration_t* enumeration) {
  return api_entry_context<tiledb::api::tiledb_array_schema_add_enumeration>(
      ctx, array_schema, enumeration);
}

CAPI_INTERFACE(
    array_schema_set_coords_filter_list,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t* filter_list) {
  return api_entry_context<
      tiledb::api::tiledb_array_schema_set_coords_filter_list>(
      ctx, array_schema, filter_list);
}

CAPI_INTERFACE(
    array_schema_set_offsets_filter_list,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t* filter_list) {
  return api_entry_context<
      tiledb::api::tiledb_array_schema_set_offsets_filter_list>(
      ctx, array_schema, filter_list);
}

CAPI_INTERFACE(
    array_schema_set_validity_filter_list,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t* filter_list) {
  return api_entry_context<
      tiledb::api::tiledb_array_schema_set_validity_filter_list>(
      ctx, array_schema, filter_list);
}

CAPI_INTERFACE(
    array_schema_check,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema) {
  return api_entry_with_context<tiledb::api::tiledb_array_schema_check>(
      ctx, array_schema);
}

CAPI_INTERFACE(
    array_schema_get_array_type,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_array_type_t* array_type) {
  return api_entry_context<tiledb::api::tiledb_array_schema_get_array_type>(
      ctx, array_schema, array_type);
}

CAPI_INTERFACE(
    array_schema_get_capacity,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    uint64_t* capacity) {
  return api_entry_context<tiledb::api::tiledb_array_schema_get_capacity>(
      ctx, array_schema, capacity);
}

CAPI_INTERFACE(
    array_schema_get_cell_order,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_layout_t* cell_order) {
  return api_entry_context<tiledb::api::tiledb_array_schema_get_cell_order>(
      ctx, array_schema, cell_order);
}

CAPI_INTERFACE(
    array_schema_get_coords_filter_list,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t** filter_list) {
  return api_entry_context<
      tiledb::api::tiledb_array_schema_get_coords_filter_list>(
      ctx, array_schema, filter_list);
}

CAPI_INTERFACE(
    array_schema_get_offsets_filter_list,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t** filter_list) {
  return api_entry_context<
      tiledb::api::tiledb_array_schema_get_offsets_filter_list>(
      ctx, array_schema, filter_list);
}

CAPI_INTERFACE(
    array_schema_get_validity_filter_list,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t** filter_list) {
  return api_entry_context<
      tiledb::api::tiledb_array_schema_get_validity_filter_list>(
      ctx, array_schema, filter_list);
}

CAPI_INTERFACE(
    array_schema_get_domain,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_domain_t** domain) {
  return api_entry_context<tiledb::api::tiledb_array_schema_get_domain>(
      ctx, array_schema, domain);
}

CAPI_INTERFACE(
    array_schema_get_tile_order,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_layout_t* tile_order) {
  return api_entry_context<tiledb::api::tiledb_array_schema_get_tile_order>(
      ctx, array_schema, tile_order);
}

CAPI_INTERFACE(
    array_schema_get_attribute_num,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    uint32_t* attribute_num) {
  return api_entry_context<tiledb::api::tiledb_array_schema_get_attribute_num>(
      ctx, array_schema, attribute_num);
}

CAPI_INTERFACE(
    array_schema_dump,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    FILE* out) {
  return api_entry_context<tiledb::api::tiledb_array_schema_dump>(
      ctx, array_schema, out);
}

CAPI_INTERFACE(
    array_schema_dump_str,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_string_t** out) {
  return api_entry_context<tiledb::api::tiledb_array_schema_dump_str>(
      ctx, array_schema, out);
}

CAPI_INTERFACE(
    array_schema_get_attribute_from_index,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    uint32_t index,
    tiledb_attribute_t** attr) {
  return api_entry_context<
      tiledb::api::tiledb_array_schema_get_attribute_from_index>(
      ctx, array_schema, index, attr);
}

CAPI_INTERFACE(
    array_schema_get_attribute_from_name,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char* name,
    tiledb_attribute_t** attr) {
  return api_entry_context<
      tiledb::api::tiledb_array_schema_get_attribute_from_name>(
      ctx, array_schema, name, attr);
}

CAPI_INTERFACE(
    array_schema_has_attribute,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char* name,
    int32_t* has_attr) {
  return api_entry_context<tiledb::api::tiledb_array_schema_has_attribute>(
      ctx, array_schema, name, has_attr);
}

CAPI_INTERFACE(
    array_schema_set_current_domain,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_current_domain_t* current_domain) {
  return api_entry_context<tiledb::api::tiledb_array_schema_set_current_domain>(
      ctx, array_schema, current_domain);
}

CAPI_INTERFACE(
    array_schema_get_current_domain,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_current_domain_t** current_domain) {
  return api_entry_context<tiledb::api::tiledb_array_schema_get_current_domain>(
      ctx, array_schema, current_domain);
}
