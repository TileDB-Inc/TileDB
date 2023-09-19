/**
 * @file
 * tiledb/api/c_api/query_field/query_field_api_external_experimental.h
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
 * This file declares the query field C API for TileDB.
 */

#ifndef TILEDB_CAPI_QUERY_FIELD_API_EXTERNAL_EXPERIMENTAL_H
#define TILEDB_CAPI_QUERY_FIELD_API_EXTERNAL_EXPERIMENTAL_H

#include "../api_external_common.h"
#include "tiledb/api/c_api/datatype/datatype_api_external.h"
#include "tiledb/api/c_api/query_aggregate/query_aggregate_api_external_experimental.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tiledb_query_field_handle_t tiledb_query_field_t;

/** TileDB query field origin. */
typedef enum {
#define TILEDB_FIELD_ORIGIN_ENUM(id) TILEDB_##id
#include "tiledb/api/c_api/query_field/query_field_api_enum.h"
#undef TILEDB_FIELD_ORIGIN_ENUM
} tiledb_field_origin_t;

/**
 * TODO
 * **Example:**
 *
 * @code{.c}
 * TODO
 * @endcode
 *
 * @param ctx The TileDB context
 * @param TODO
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_query_get_field(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* field_name,
    tiledb_query_field_t** field) TILEDB_NOEXCEPT;

/**
 * TODO
 * **Example:**
 *
 * @code{.c}
 * TODO
 * @endcode
 *
 * @param ctx The TileDB context
 * @param TODO
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_query_field_free(
    tiledb_ctx_t* ctx, tiledb_query_field_t** field) TILEDB_NOEXCEPT;

/**
 * TODO
 * **Example:**
 *
 * @code{.c}
 * TODO
 * @endcode
 *
 * @param ctx The TileDB context
 * @param TODO
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_field_type(
    tiledb_ctx_t* ctx,
    tiledb_query_field_t* field,
    tiledb_datatype_t* type) TILEDB_NOEXCEPT;

/**
 * TODO
 * **Example:**
 *
 * @code{.c}
 * TODO
 * @endcode
 *
 * @param ctx The TileDB context
 * @param TODO
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_field_cell_val_num(
    tiledb_ctx_t* ctx,
    tiledb_query_field_t* field,
    uint32_t* cell_val_num) TILEDB_NOEXCEPT;

/**
 * TODO
 * **Example:**
 *
 * @code{.c}
 * TODO
 * @endcode
 *
 * @param ctx The TileDB context
 * @param TODO
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_field_origin(
    tiledb_ctx_t* ctx,
    tiledb_query_field_t* field,
    tiledb_field_origin_t* origin) TILEDB_NOEXCEPT;

/**
 * TODO
 * **Example:**
 *
 * @code{.c}
 * TODO
 * @endcode
 *
 * @param ctx The TileDB context
 * @param TODO
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_field_channel(
    tiledb_ctx_t* ctx,
    tiledb_query_field_t* field,
    tiledb_query_channel_t** channel) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_QUERY_FIELD_API_EXTERNAL_EXPERIMENTAL_H
