/**
 * @file tiledb/api/c_api/group/group_api_external.h
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
 * This file declares the group C API for TileDB.
 */

#ifndef TILEDB_CAPI_GROUP_API_EXTERNAL_H
#define TILEDB_CAPI_GROUP_API_EXTERNAL_H

#include "../api_external_common.h"
#include "tiledb/api/c_api/context/context_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/** A group object. */
typedef struct tiledb_group_handle_t tiledb_group_t;

/**
 * Creates a new TileDB group.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_group_create(ctx, "my_group");
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group_uri The group URI.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t
tiledb_group_create(tiledb_ctx_t* ctx, const char* group_uri) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_GROUP_API_EXTERNAL_H
