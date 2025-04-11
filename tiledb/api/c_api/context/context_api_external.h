/**
 * @file tiledb/api/c_api/context/context_api_external.h
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

#ifndef TILEDB_CAPI_CONTEXT_EXTERNAL_H
#define TILEDB_CAPI_CONTEXT_EXTERNAL_H

#include "../api_external_common.h"
#include "../config/config_api_external.h"
#include "../filesystem/filesystem_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * C API carrier for a TileDB context
 */
typedef struct tiledb_ctx_handle_t tiledb_ctx_t;

/**
 * Creates a TileDB context, which contains the TileDB storage manager
 * that manages everything in the TileDB library.
 *
 * **Examples:**
 *
 * Without config (i.e., use default configuration):
 *
 * @code{.c}
 * tiledb_ctx_t* ctx;
 * tiledb_ctx_alloc(NULL, &ctx);
 * @endcode
 *
 * With some config:
 *
 * @code{.c}
 * tiledb_ctx_t* ctx;
 * tiledb_ctx_alloc(config, &ctx);
 * @endcode
 *
 * @param config The configuration parameters (`NULL` means default).
 * @param ctx The TileDB context to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t
tiledb_ctx_alloc(tiledb_config_t* config, tiledb_ctx_t** ctx) TILEDB_NOEXCEPT;

/**
 * Destroys the TileDB context, freeing all associated memory and resources.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_ctx_t* ctx;
 * tiledb_ctx_alloc(NULL, &ctx);
 * tiledb_ctx_free(&ctx);
 * @endcode
 *
 * @param ctx The TileDB context to be freed.
 */
TILEDB_EXPORT void tiledb_ctx_free(tiledb_ctx_t** ctx) TILEDB_NOEXCEPT;

/**
 * Retrieves the stats from a TileDB context.
 *
 * **Example:**
 *
 * @code{.c}
 * char* stats_json;
 * tiledb_ctx_get_stats(ctx, &stats_json);
 * // Use the string
 * tiledb_stats_free_str(&stats_json);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param stats_json The output json. The caller takes ownership
 *   of the c-string and must free it using tiledb_stats_free_str().
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t
tiledb_ctx_get_stats(tiledb_ctx_t* ctx, char** stats_json) TILEDB_NOEXCEPT;

/**
 * Retrieves a copy of the config from a TileDB context.
 * Modifying this config will not affect the initialized
 * context configuration.
 *
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_config_t* config;
 * tiledb_ctx_get_config(ctx, &config);
 * // Make sure to free the retrieved config
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param config The config to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_ctx_get_config(
    tiledb_ctx_t* ctx, tiledb_config_t** config) TILEDB_NOEXCEPT;

/**
 * Retrieves the last TileDB error associated with a TileDB context.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_error_t* error;
 * tiledb_ctx_get_last_error(ctx, &error);
 * // Make sure to free the retrieved error, checking first if it is NULL
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param err The last error, or `NULL` if no error has been raised.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_ctx_get_last_error(
    tiledb_ctx_t* ctx, tiledb_error_t** err) TILEDB_NOEXCEPT;

/**
 * Checks if a given storage filesystem backend is supported.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t is_supported;
 * tiledb_ctx_is_supported_fs(ctx, TILEDB_S3, &is_supported);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param fs The filesystem to be checked.
 * @param is_supported Sets it to `1` if the filesystem is supported, and
 * `0` otherwise.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_ctx_is_supported_fs(
    tiledb_ctx_t* ctx,
    tiledb_filesystem_t fs,
    int32_t* is_supported) TILEDB_NOEXCEPT;

/**
 * Cancels all background or async tasks associated with the given context.
 *
 * @param ctx The TileDB context.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_ctx_cancel_tasks(tiledb_ctx_t* ctx)
    TILEDB_NOEXCEPT;

/**
 * Sets a string key-value "tag" on the given context.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_ctx_set_tag(ctx, "tag key", "tag value");
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param key The tag key
 * @param value The tag value.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_ctx_set_tag(
    tiledb_ctx_t* ctx, const char* key, const char* value) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_CONTEXT_EXTERNAL_H
