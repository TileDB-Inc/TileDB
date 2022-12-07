/**
 * @file tiledb/api/c_api/filesystem/filesystem_api_external.h
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
 * This file declares the C API for TileDB.
 */

#ifndef TILEDB_CAPI_FILESYSTEM_API_EXTERNAL_H
#define TILEDB_CAPI_FILESYSTEM_API_EXTERNAL_H

#include "../api_external_common.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Filesystem type.
 */
typedef enum {
/** Helper macro for defining filesystem enums. */
#define TILEDB_FILESYSTEM_ENUM(id) TILEDB_##id
#include "tiledb/api/c_api/filesystem/filesystem_api_enum.h"
#undef TILEDB_FILESYSTEM_ENUM
} tiledb_filesystem_t;

/**
 * Returns a string representation of the given filesystem.
 *
 * @param filesystem Filesystem
 * @param str Set to point to a constant string representation of the filesystem
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filesystem_to_str(
    tiledb_filesystem_t filesystem, const char** str) TILEDB_NOEXCEPT;

/**
 * Parses a filesystem from the given string.
 *
 * @param str String representation to parse
 * @param filesystem Set to the parsed filesystem
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filesystem_from_str(
    const char* str, tiledb_filesystem_t* filesystem) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_FILESYSTEM_API_EXTERNAL_H
