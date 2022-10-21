/**
 * @file tiledb/api/c_api/error/error_api_external.h
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
 * This file declares the error section of the C API for TileDB.
 */

#ifndef TILEDB_CAPI_ERROR_EXTERNAL_H
#define TILEDB_CAPI_ERROR_EXTERNAL_H

#include "../api_external_common.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * C API carrier for a TileDB error
 */
typedef struct tiledb_error_handle_t tiledb_error_t;

/**
 * Returns the error message associated with a TileDB error object.
 *
 * **Example:**
 *
 * The following shows how to get the last error from a TileDB context. If the
 * error does not contain an error message ``errmsg`` is set to ``NULL``.
 *
 * @code{.c}
 * tiledb_error_t* err = NULL;
 * tiledb_ctx_get_last_error(ctx, &err);
 * const char* msg;
 * tiledb_error_message(err, &msg);
 * printf("%s\n", msg);
 * @endcode
 *
 * @param err A TileDB error object.
 * @param errmsg A constant pointer to the error message.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t
tiledb_error_message(tiledb_error_t* err, const char** errmsg) TILEDB_NOEXCEPT;

/**
 * Frees the resources associated with a TileDB error object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_error_t* err = NULL;
 * tiledb_ctx_get_last_error(ctx, &err);
 * const char* msg;
 * tiledb_error_message(err, &msg);
 * printf("%s\n", msg);
 * tiledb_error_free(&err);
 * @endcode
 *
 * @param err The TileDB error object.
 */
TILEDB_EXPORT void tiledb_error_free(tiledb_error_t** err) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_ERROR_EXTERNAL_H
