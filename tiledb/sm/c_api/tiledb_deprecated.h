/*
 * @file   tiledb_deprecated.h
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
 * This file declares the deprecated C API for TileDB.
 */

#ifndef TILEDB_DEPRECATED_H
#define TILEDB_DEPRECATED_H

#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

// No deprecated APIs are declared at the moment.

/**
 * Dumps the array schema in ASCII format in the selected file output.
 *
 * **Example:**
 *
 * The following prints the array schema dump in standard output.
 *
 * @code{.c}
 * tiledb_array_schema_dump(ctx, array_schema, stdout);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param out The output handle.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_schema_dump(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    FILE* out) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_DEPRECATED_H
