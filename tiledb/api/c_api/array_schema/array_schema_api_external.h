/**
 * @file tiledb/api/c_api/array_schema/array_schema_api_external.h
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
 * This file declares the ArraySchema C API for TileDB.
 */

#ifndef TILEDB_CAPI_ARRAY_SCHEMA_EXTERNAL_H
#define TILEDB_CAPI_ARRAY_SCHEMA_EXTERNAL_H

#include "../api_external_common.h"
#include "../attribute/attribute_api_external.h"
#include "../context/context_api_external.h"
#include "../domain/domain_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/** C API carrier for a TileDB array schema */
typedef struct tiledb_array_schema_handle_t tiledb_array_schema_t;

/** Array type. */
typedef enum {
/** Helper macro for defining array type enums. */
#define TILEDB_ARRAY_TYPE_ENUM(id) TILEDB_##id
#include "tiledb/api/c_api/array_schema/array_type_enum.h"
#undef TILEDB_ARRAY_TYPE_ENUM
} tiledb_array_type_t;

/** Tile or cell layout. */
typedef enum {
/** Helper macro for defining layout type enums. */
#define TILEDB_LAYOUT_ENUM(id) TILEDB_##id
#include "tiledb/api/c_api/array_schema/layout_enum.h"
#undef TILEDB_LAYOUT_ENUM
} tiledb_layout_t;

/**
 * Returns a string representation of the given array type.
 *
 * @param[in] array_type Array type
 * @param[out] str A constant string representation of the array type
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_type_to_str(
    tiledb_array_type_t array_type, const char** str) TILEDB_NOEXCEPT;

/**
 * Parses an array type from the given string.
 *
 * @param[in] str String representation to parse
 * @param[out] array_type The parsed array type
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_type_from_str(
    const char* str, tiledb_array_type_t* array_type) TILEDB_NOEXCEPT;

/**
 * Returns a string representation of the given layout.
 *
 * @param[in] layout Layout
 * @param[out] str A constant string representation of the layout
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t
tiledb_layout_to_str(tiledb_layout_t layout, const char** str) TILEDB_NOEXCEPT;

/**
 * Parses a layout from the given string.
 *
 * @param[in] str String representation to parse
 * @param[out] layout The parsed layout
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_layout_from_str(
    const char* str, tiledb_layout_t* layout) TILEDB_NOEXCEPT;

/**
 * Creates a TileDB array schema object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_t* array_schema;
 * tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &array_schema);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_type The array type.
 * @param[out] array_schema The TileDB array schema to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_alloc(
    tiledb_ctx_t* ctx,
    tiledb_array_type_t array_type,
    tiledb_array_schema_t** array_schema) TILEDB_NOEXCEPT;

/**
 * Creates a TileDB array schema object with specified creation time.
 *
 * Note: This is an experimental API for expert users only. This should be used
 * with caution only as needed. Setting custom timestamps in general increases
 * the likelihood of misuse and potential data corruption. This API intended to
 * be used for handling schema evolution at custom timestamps.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_t* array_schema;
 * uint64_t t1 = 10;
 * uint64_t t2 = 20;
 * tiledb_array_schema_alloc_at_timestamp(ctx, TILEDB_DENSE, t1, t2,
 * &array_schema);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_type The array type.
 * @param[out] array_schema The TileDB array schema to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_alloc_at_timestamp(
    tiledb_ctx_t* ctx,
    tiledb_array_type_t array_type,
    uint64_t t1,
    uint64_t t2,
    tiledb_array_schema_t** array_schema) TILEDB_NOEXCEPT;

/**
 * Destroys an array schema, freeing associated memory.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_free(&array_schema);
 * @endcode
 *
 * @param[in] array_schema The array schema to be destroyed.
 */
TILEDB_EXPORT void tiledb_array_schema_free(
    tiledb_array_schema_t** array_schema) TILEDB_NOEXCEPT;

/**
 * Adds an attribute to an array schema.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_attribute_t* attr;
 * tiledb_attribute_alloc(ctx, "my_attr", TILEDB_INT32, &attr);
 * tiledb_array_schema_add_attribute(ctx, array_schema, attr);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[in] attr The attribute to be added.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_attribute_t* attr) TILEDB_NOEXCEPT;

/**
 * Sets whether the array can allow coordinate duplicates or not.
 * Applicable only to sparse arrays (it errors out if set to `1` for dense
 * arrays).
 *
 * **Example:**
 *
 * @code{.c}
 * int allows_dups = 1;
 * tiledb_array_schema_set_allows_dups(ctx, array_schema, allows_dups);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[in] allows_dups Whether or not the array allows coordinate duplicates.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_set_allows_dups(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    int allows_dups) TILEDB_NOEXCEPT;

/**
 * Gets whether the array can allow coordinate duplicates or not.
 * It should always be `0` for dense arrays.
 *
 * **Example:**
 *
 * @code{.c}
 * int allows_dups;
 * tiledb_array_schema_get_allows_dups(ctx, array_schema, &allows_dups);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[out] allows_dups Whether or not the array allows coordinate
 * duplicates.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_get_allows_dups(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    int* allows_dups) TILEDB_NOEXCEPT;

/**
 * Returns the array schema version.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t version;
 * tiledb_array_schema_get_version(ctx, array_schema, &version);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[out] version The version.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_get_version(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    uint32_t* version) TILEDB_NOEXCEPT;

/**
 * Sets a domain for the array schema.
 *
 * @pre The domain has at least one dimension set.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_domain_t* domain;
 * tiledb_domain_alloc(ctx, &domain);
 * // -- Add dimensions to the domain here -- //
 * tiledb_array_schema_set_domain(ctx, array_schema, domain);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[in] domain The domain to be set.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_set_domain(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_domain_t* domain) TILEDB_NOEXCEPT;

/**
 * Sets the tile capacity. Applies to sparse arrays only.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_set_capacity(ctx, array_schema, 10000);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[in] capacity The capacity of a sparse data tile. Note that
 * sparse data tiles exist in sparse fragments, which can be created
 * in sparse arrays only. For more details,
 * see [tutorials/tiling-sparse.html](tutorials/tiling-sparse.html).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_set_capacity(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    uint64_t capacity) TILEDB_NOEXCEPT;

/**
 * Sets the cell order.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[in] cell_order The cell order to be set.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_set_cell_order(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_layout_t cell_order) TILEDB_NOEXCEPT;

/**
 * Sets the tile order.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_COL_MAJOR);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[in] tile_order The tile order to be set.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_set_tile_order(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_layout_t tile_order) TILEDB_NOEXCEPT;

/**
 * Sets the filter list to use for the coordinates.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_list_t* filter_list;
 * tiledb_filter_list_alloc(ctx, &filter_list);
 * tiledb_filter_list_add_filter(ctx, filter_list, filter);
 * tiledb_array_schema_set_coords_filter_list(ctx, array_schema, filter_list);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[in] filter_list The filter list to be set.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_set_coords_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t* filter_list) TILEDB_NOEXCEPT;

/**
 * Sets the filter list to use for the offsets of variable-sized attribute
 * values.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_list_t* filter_list;
 * tiledb_filter_list_alloc(ctx, &filter_list);
 * tiledb_filter_list_add_filter(ctx, filter_list, filter);
 * tiledb_array_schema_set_offsets_filter_list(ctx, array_schema, filter_list);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[in] filter_list The filter list to be set.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_set_offsets_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t* filter_list) TILEDB_NOEXCEPT;

/**
 * Sets the filter list to use for the validity array of nullable attribute
 * values.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_list_t* filter_list;
 * tiledb_filter_list_alloc(ctx, &filter_list);
 * tiledb_filter_list_add_filter(ctx, filter_list, filter);
 * tiledb_array_schema_set_validity_filter_list(ctx, array_schema, filter_list);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[in] filter_list The filter list to be set.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_set_validity_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t* filter_list) TILEDB_NOEXCEPT;

/**
 * Checks the correctness of the array schema.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_check(ctx, array_schema);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @return `TILEDB_OK` if the array schema is correct and `TILEDB_ERR` upon any
 *     error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_check(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema) TILEDB_NOEXCEPT;

/**
 * Retrieves the array type.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_t* array_schema;
 * tiledb_array_schema_load(ctx, "s3://tiledb_bucket/my_array", array_schema);
 * tiledb_array_type_t* array_type;
 * tiledb_array_schema_get_array_type(ctx, array_schema, &array_type);
 * // Make sure to free the array schema in the end
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[out] array_type The array type to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_get_array_type(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_array_type_t* array_type) TILEDB_NOEXCEPT;

/**
 * Retrieves the capacity.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t capacity;
 * tiledb_array_schema_get_capacity(ctx, array_schema, &capacity);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[out] capacity The capacity to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_get_capacity(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    uint64_t* capacity) TILEDB_NOEXCEPT;

/**
 * Retrieves the cell order.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_layout_t cell_order;
 * tiledb_array_schema_get_cell_order(ctx, array_schema, &cell_order);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[out] cell_order The cell order to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_get_cell_order(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_layout_t* cell_order) TILEDB_NOEXCEPT;

/**
 * Retrieves the filter list used for the coordinates.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_list_t* filter_list;
 * tiledb_array_schema_get_coords_filter_list(ctx, array_schema, &filter_list);
 * tiledb_filter_list_free(ctx, &filter_list);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[out] filter_list The filter list to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_get_coords_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t** filter_list) TILEDB_NOEXCEPT;

/**
 * Retrieves the filter list used for the offsets.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_list_t* filter_list;
 * tiledb_array_schema_get_offsets_filter_list(ctx, array_schema, &filter_list);
 * tiledb_filter_list_free(ctx, &filter_list);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[out] filter_list The filter list to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_get_offsets_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t** filter_list) TILEDB_NOEXCEPT;

/**
 * Retrieves the filter list used for validity maps.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_list_t* filter_list;
 * tiledb_array_schema_get_validity_filter_list(
 *  ctx, array_schema, &filter_list);
 * tiledb_filter_list_free(ctx, &filter_list);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[out] filter_list The filter list to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_get_validity_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t** filter_list) TILEDB_NOEXCEPT;

/**
 * Retrieves the array domain.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_domain_t* domain;
 * tiledb_array_schema_get_domain(ctx, array_schema, &domain);
 * tiledb_domain_free(&domain);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[out] domain The array domain to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_get_domain(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_domain_t** domain) TILEDB_NOEXCEPT;

/**
 * Retrieves the tile order.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_layout_t tile_order;
 * tiledb_array_schema_get_tile_order(ctx, array_schema, &tile_order);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[out] tile_order The tile order to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_get_tile_order(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_layout_t* tile_order) TILEDB_NOEXCEPT;

/**
 * Retrieves the number of array attributes.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t attr_num;
 * tiledb_array_schema_get_attribute_num(ctx, array_schema, &attr_num);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[out] attribute_num The number of attributes to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_get_attribute_num(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    uint32_t* attribute_num) TILEDB_NOEXCEPT;

/**
 * Retrieves an attribute given its index.
 *
 * Attributes are ordered the same way they were defined
 * when constructing the array schema.
 *
 * **Example:**
 *
 * The following retrieves the first attribute in the schema.
 *
 * @code{.c}
 * tiledb_attribute_t* attr;
 * tiledb_array_schema_get_attribute_from_index(ctx, array_schema, 0, &attr);
 * tiledb_attribute_free(&attr);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[in] index The index of the attribute to retrieve.
 * @param[out] attr The attribute object to retrieve.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_get_attribute_from_index(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    uint32_t index,
    tiledb_attribute_t** attr) TILEDB_NOEXCEPT;

/**
 * Retrieves an attribute given its name (key).
 *
 * **Example:**
 *
 * The following retrieves the attribute named "a" in the schema.
 *
 * @code{.c}
 * tiledb_attribute_t* attr;
 * tiledb_array_schema_get_attribute_from_name(ctx, array_schema, "a", &attr);
 * tiledb_attribute_free(&attr);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[in] name The name (key) of the attribute to retrieve.
 * @param[out] attr The attribute object to retrieve.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_get_attribute_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char* name,
    tiledb_attribute_t** attr) TILEDB_NOEXCEPT;

/**
 * Checks whether the array schema has an attribute of the given name.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t has_attr;
 * tiledb_array_schema_has_attribute(ctx, array_schema, "attr_0", &has_attr);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[in] name The name of the attribute to check for.
 * @param[out] has_attr Set to `1` if the array schema has an attribute of the
 *      given name, else `0`.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_has_attribute(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char* name,
    int32_t* has_attr) TILEDB_NOEXCEPT;

/**
 * Dumps the array schema in ASCII format in the selected string output.
 *
 * The output string handle must be freed by the user after use.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_string_t* tdb_string;
 * tiledb_array_schema_dump_str(ctx, array_schema, &tdb_string);
 * // Use the string
 * tiledb_string_free(&tdb_string);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[out] out The output string.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_dump_str(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_string_t** out) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_ARRAY_SCHEMA_EXTERNAL_H
