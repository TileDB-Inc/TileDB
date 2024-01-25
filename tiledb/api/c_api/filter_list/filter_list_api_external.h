/**
 * @file tiledb/api/c_api/filter_list/filter_list_api_external.h
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

#ifndef TILEDB_CAPI_FILTER_LIST_EXTERNAL_H
#define TILEDB_CAPI_FILTER_LIST_EXTERNAL_H

#include "../api_external_common.h"
#include "../context/context_api_external.h"
#include "../filter/filter_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * C API carrier for a TileDB filter
 */
typedef struct tiledb_filter_list_handle_t tiledb_filter_list_t;

/**
 * Creates a TileDB filter list (pipeline of filters).
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_list_t* filter_list;
 * tiledb_filter_list_alloc(ctx, &filter_list);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param filter_list The TileDB filter list to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filter_list_alloc(
    tiledb_ctx_t* ctx, tiledb_filter_list_t** filter_list) TILEDB_NOEXCEPT;

/**
 * Destroys a TileDB filter list, freeing associated memory.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_list_t* filter_list;
 * tiledb_filter_list_alloc(ctx, &filter_list);
 * tiledb_filter_list_free(&filter_list);
 * @endcode
 *
 * @param filter_list The filter list to be destroyed.
 */
TILEDB_EXPORT void tiledb_filter_list_free(tiledb_filter_list_t** filter_list)
    TILEDB_NOEXCEPT;

/**
 * Appends a filter to a filter list. Data is processed through each filter in
 * the order the filters were added.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_t* filter;
 * tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter);
 *
 * tiledb_filter_list_t* filter_list;
 * tiledb_filter_list_alloc(ctx, &filter_list);
 * tiledb_filter_list_add_filter(ctx, filter_list, filter);
 *
 * tiledb_filter_list_free(&filter_list);
 * tiledb_filter_free(&filter);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param filter_list The target filter list.
 * @param filter The filter to add.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filter_list_add_filter(
    tiledb_ctx_t* ctx,
    tiledb_filter_list_t* filter_list,
    tiledb_filter_t* filter) TILEDB_NOEXCEPT;

/**
 * Sets the maximum tile chunk size for a filter list.
 *
 * **Example:**
 *
 * @code{.c}
 * // Set max chunk size to 16KB:
 * tiledb_filter_list_set_max_chunk_size(ctx, filter_list, 16384);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param filter_list The target filter list
 * @param max_chunk_size The max chunk size value to set
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filter_list_set_max_chunk_size(
    tiledb_ctx_t* ctx,
    tiledb_filter_list_t* filter_list,
    uint32_t max_chunk_size) TILEDB_NOEXCEPT;

/**
 * Retrieves the number of filters in a filter list.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t num_filters;
 * tiledb_filter_list_get_nfilters(ctx, filter_list, &num_filters);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param filter_list The filter list
 * @param nfilters The number of filters on the filter list
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filter_list_get_nfilters(
    tiledb_ctx_t* ctx,
    const tiledb_filter_list_t* filter_list,
    uint32_t* nfilters) TILEDB_NOEXCEPT;

/**
 * Retrieves a filter object from a filter list by index.
 *
 * **Example:**
 *
 * The following retrieves the first filter from a filter list.
 *
 * @code{.c}
 * tiledb_filter_t* filter;
 * tiledb_filter_list_get_filter_from_index(ctx, filter_list, 0, &filter);
 * tiledb_filter_free(&filter);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param filter_list The filter list to retrieve the filter from
 * @param index The index of the filter
 * @param filter The retrieved filter object.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filter_list_get_filter_from_index(
    tiledb_ctx_t* ctx,
    const tiledb_filter_list_t* filter_list,
    uint32_t index,
    tiledb_filter_t** filter) TILEDB_NOEXCEPT;

/**
 * Gets the maximum tile chunk size for a filter list.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t max_chunk_size;
 * tiledb_filter_list_get_max_chunk_size(ctx, filter_list, &max_chunk_size);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param filter_list The target filter list
 * @param max_chunk_size The retrieved max chunk size value
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filter_list_get_max_chunk_size(
    tiledb_ctx_t* ctx,
    const tiledb_filter_list_t* filter_list,
    uint32_t* max_chunk_size) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_FILTER_LIST_EXTERNAL_H
