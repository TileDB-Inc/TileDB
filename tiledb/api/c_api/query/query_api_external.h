/**
 * @file tiledb/api/c_api/query/query_api_external.h
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
 * This file declares the query C API for TileDB.
 */

#ifndef TILEDB_CAPI_QUERY_API_EXTERNAL_H
#define TILEDB_CAPI_QUERY_API_EXTERNAL_H

#include "../api_external_common.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tiledb_query_t tiledb_query_t;

/** TileDB query type. */
typedef enum {
/** Helper macro for defining query type enums. */
#define TILEDB_QUERY_TYPE_ENUM(id) TILEDB_##id
// Use token-pasting _CONCAT_ID version for precarious symbols
// (such as DELETE, see `query_api_enum.h`)
#define TILEDB_QUERY_TYPE_ENUM_CONCAT_ID(id, id2) TILEDB_##id##id2
#include "tiledb/api/c_api/query/query_api_enum.h"
#undef TILEDB_QUERY_TYPE_ENUM
#undef TILEDB_QUERY_TYPE_ENUM_CONCAT_ID
} tiledb_query_type_t;

/**
 * Returns a string representation of the given query type.
 *
 * @param query_type Query type
 * @param str Set to point to a constant string representation of the query type
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_query_type_to_str(
    tiledb_query_type_t query_type, const char** str) TILEDB_NOEXCEPT;

/**
 * Parses a query type from the given string.
 *
 * @param str String representation to parse
 * @param query_type Set to the parsed query type
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_query_type_from_str(
    const char* str, tiledb_query_type_t* query_type) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_QUERY_API_EXTERNAL_H
