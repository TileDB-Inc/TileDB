/**
 * @file tiledb/api/c_api/context/context_api_experimentnal.h
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
 * This file declares the experimental C API for TileDB context.
 */

#ifndef TILEDB_CAPI_CONTEXT_EXPERIMENTAL_H
#define TILEDB_CAPI_CONTEXT_EXPERIMENTAL_H

#include "../api_external_common.h"
#include "../error/error_api_external.h"
#include "context_api_external.h"

#include "../config/config_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Creates a TileDB context, which contains the TileDB storage manager
 * that manages everything in the TileDB library. This is a provisional API
 * which returns an error object when the context creation fails. This API will
 * be replaced with a more proper "v2" of context alloc in the near future. The
 * main goal is to use to this to capture potential failures to inform the v2
 * alloc design.
 *
 * **Examples:**
 *
 * Without config (i.e., use default configuration):
 *
 * @code{.c}
 * tiledb_ctx_t* ctx;
 * tiledb_error_t* error;
 * tiledb_ctx_alloc_with_error(NULL, &ctx, &error);
 * @endcode
 *
 * With some config:
 *
 * @code{.c}
 * tiledb_ctx_t* ctx;
 * tiledb_error_t* error;
 * tiledb_ctx_alloc_with_error(config, &ctx, &error);
 * @endcode
 *
 * @param[in] config The configuration parameters (`NULL` means default).
 * @param[out] ctx The TileDB context to be created.
 * @param[out] error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_ctx_alloc_with_error(
    tiledb_config_t* config,
    tiledb_ctx_t** ctx,
    tiledb_error_t** error) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_CONTEXT_EXPERIMENTAL_H
