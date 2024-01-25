/**
 * @file tiledb/api/c_api/string/string_api_external.h
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
 * This file declares the string section of the C API for TileDB.
 */

#ifndef TILEDB_CAPI_STRING_EXTERNAL_H
#define TILEDB_CAPI_STRING_EXTERNAL_H

#include "../api_external_common.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * C API carrier for a TileDB string
 *
 * TileDB strings are designed as output-only strings. They hold string values
 * that API function return through pointer-typed output arguments. The
 * interface is, by design, insufficient for use as input arguments; for these,
 * ordinary `char *` strings suffice.
 */
typedef struct tiledb_string_handle_t tiledb_string_t;

/**
 * Returns a view (i.e. data and length) of a TileDB string object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_string_t* s = NULL;
 * // tiledb_something_outputs_a_string(..., &s);
 * const char* data;
 * size_t length;
 * tiledb_string_view(s, &data, &length);
 * printf("\"%s\" has length %d\n", data, length);
 * tiledb_string_free(&s);
 * @endcode
 *
 * @param s A TileDB string object
 * @param[out] data The contents of the string
 * @param[out] length The length of the string
 */
TILEDB_EXPORT capi_return_t tiledb_string_view(
    tiledb_string_t* s, const char** data, size_t* length) TILEDB_NOEXCEPT;

/**
 * Frees the resources associated with a TileDB string object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_string_t* s = NULL;
 * // tiledb_something_outputs_a_string(..., &s);
 * tiledb_string_free(&s);
 * @endcode
 *
 * @param s A TileDB string object
 */
TILEDB_EXPORT capi_return_t tiledb_string_free(tiledb_string_t** s)
    TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_STRING_EXTERNAL_H
