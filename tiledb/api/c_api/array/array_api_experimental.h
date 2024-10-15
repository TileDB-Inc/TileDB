/**
 * @file tiledb/api/c_api/array/array_api_experimental.h
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
 * This file declares the array section of the experimental TileDB C API.
 */

#ifndef TILEDB_CAPI_ARRAY_EXPERIMENTAL_H
#define TILEDB_CAPI_ARRAY_EXPERIMENTAL_H

#include "../api_external_common.h"
#include "array_api_external.h"
#include "tiledb/api/c_api/array_schema_evolution/array_schema_evolution_api_experimental.h"
#include "tiledb/api/c_api/enumeration/enumeration_api_experimental.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Evolve array schema of an array.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* array_uri="test_array";
 * tiledb_array_evolve(ctx, array_uri,array_schema_evolution);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_uri The uri of the array.
 * @param[in] array_schema_evolution The schema evolution.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_evolve(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_evolution_t* array_schema_evolution) TILEDB_NOEXCEPT;

/**
 * Retrieves an attribute's enumeration given the attribute name (key).
 *
 * **Example:**
 *
 * The following retrieves the first attribute in the schema.
 *
 * @code{.c}
 * tiledb_attribute_t* attr;
 * tiledb_array_schema_get_enumeration(
 *     ctx, array_schema, "attr_0", &enumeration);
 * // Make sure to delete the retrieved attribute in the end.
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array The TileDB array.
 * @param[in] name The name (key) of the attribute from which to
 * retrieve the enumeration.
 * @param[out] enumeration The enumeration object to retrieve.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_get_enumeration(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    const char* name,
    tiledb_enumeration_t** enumeration) TILEDB_NOEXCEPT;

/**
 * Load all enumerations for the array's latest array schema.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_load_all_enumerations(ctx, array);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array The TileDB array.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_load_all_enumerations(
    tiledb_ctx_t* ctx, const tiledb_array_t* array) TILEDB_NOEXCEPT;

/**
 * Load all enumerations for all schemas in the array.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_load_enumerations_all_schemas(ctx, array);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array The TileDB array.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_load_enumerations_all_schemas(
    tiledb_ctx_t* ctx, const tiledb_array_t* array) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_ARRAY_EXPERIMENTAL_H
