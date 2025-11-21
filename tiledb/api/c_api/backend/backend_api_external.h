/**
 * @file tiledb/api/c_api/backend/backend_api_external.h
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
 * This file declares the backend type enum for TileDB C API.
 */

#ifndef TILEDB_CAPI_BACKEND_API_EXTERNAL_H
#define TILEDB_CAPI_BACKEND_API_EXTERNAL_H

#include "../api_external_common.h"
#include "../context/context_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Backend type.
 */
typedef enum {
/** Helper macro for defining backend enums. */
#define TILEDB_DATA_PROTOCOL_ENUM(id) TILEDB_BACKEND_##id
#include "tiledb/api/c_api/backend/data_protocol_api_enum.h"
#undef TILEDB_DATA_PROTOCOL_ENUM
} tiledb_data_protocol_t;

/**
 * Returns the backend type for a given URI.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_data_protocol_t backend;
 * tiledb_uri_get_data_protocol(ctx, "s3://bucket/path", &backend);
 * // backend == TILEDB_BACKEND_S3
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param uri The URI string to check.
 * @param uri_backend Set to the backend type of the URI.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_uri_get_data_protocol(
    tiledb_ctx_t* ctx,
    const char* uri,
    tiledb_data_protocol_t* uri_backend) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_BACKEND_API_EXTERNAL_H
