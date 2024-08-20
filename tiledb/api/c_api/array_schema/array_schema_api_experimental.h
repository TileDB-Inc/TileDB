/**
 * @file tiledb/api/c_api/array_schema/array_schema_api_experimental.h
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
 * This file declares the array schema section of the experimental TileDB C API.
 */

#ifndef TILEDB_CAPI_ARRAY_SCHEMA_EXPERIMENTAL_H
#define TILEDB_CAPI_ARRAY_SCHEMA_EXPERIMENTAL_H

#include "../api_external_common.h"
#include "array_schema_api_external.h"

#include "tiledb/api/c_api/current_domain/current_domain_api_external_experimental.h"
#include "tiledb/api/c_api/enumeration/enumeration_api_experimental.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Gets timestamp range in an array schema
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t timestamp_lo = 0;
 * uint64_t timestamp_hi = 0;
 * tiledb_array_schema_timestamp_range(
 *      ctx, array_schema, &timestamp_lo, &timestamp_hi);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema object.
 * @param[out] lo The lower bound of timestamp range.
 * @param[out] hi The upper bound of timestamp range.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_timestamp_range(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    uint64_t* lo,
    uint64_t* hi) TILEDB_NOEXCEPT;

/**
 * Adds an enumeration to an array schema.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_enumeration_t* enumeration;
 * tiledb_enumeration_alloc(
 *     ctx,
 *     "enumeration_name",
 *     TILEDB_INT64,
 *     1,
 *     FALSE,
 *     data,
 *     data_size,
 *     nullptr,
 *     0,
 *     &enumeration);
 * tiledb_array_schema_add_enumeration(ctx, array_schema, enumeration);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[in] enumeration The enumeration to add with the attribute
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_add_enumeration(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_enumeration_t* enumeration) TILEDB_NOEXCEPT;

/**
 * Sets the current domain on the array schema.
 *
 * @pre The schema is sparse. current_domain is not yet supported on dense
 * arrays.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_current_domain_t *current_domain;
 * tiledb_current_domain_create(ctx, &current_domain);
 * tiledb_array_schema_set_current_domain(ctx, array_schema, current_domain);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[in] current_domain The current domain to set on the schema
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_set_current_domain(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_current_domain_t* current_domain) TILEDB_NOEXCEPT;

/**
 * Gets the current domain set on the array schema or
 * creates an empty current domain if none was set.
 * It is the responsability of the caller to free the resources associated
 * with the current domain when the handle isn't needed anymore.
 *
 * @pre The schema is sparse. current_domain is not yet supported on dense
 * arrays.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_current_domain_t *current_domain;
 * tiledb_array_schema_get_current_domain(ctx, array_schema, &current_domain);
 * tiledb_current_domain_free(&current_domain);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The array schema.
 * @param[out] current_domain The current domain set on the schema
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_get_current_domain(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_current_domain_t** current_domain) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_ARRAY_SCHEMA_EXPERIMENTAL_H
