/**
 * @file tiledb/api/c_api/buffer_list/buffer_list_api_external.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file declares the buffer list C API for TileDB.
 */

#ifndef TILEDB_CAPI_BUFFER_LIST_API_EXTERNAL_H
#define TILEDB_CAPI_BUFFER_LIST_API_EXTERNAL_H

#include "../api_external_common.h"
#include "../buffer/buffer_api_external.h"
#include "../context/context_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/** A buffer object. */
typedef struct tiledb_buffer_list_handle_t tiledb_buffer_list_t;

/**
 * Creates an empty buffer list object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_buffer_list_t* buffer_list;
 * tiledb_buffer_list_alloc(ctx, &buffer_list);
 * @endcode
 *
 * @param ctx TileDB context
 * @param buffer_list The buffer list to be created
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_buffer_list_alloc(
    tiledb_ctx_t* ctx, tiledb_buffer_list_t** buffer_list) TILEDB_NOEXCEPT;

/**
 * Destroys a TileDB buffer list, freeing associated memory.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_buffer_t* buffer_list;
 * tiledb_buffer_list_alloc(ctx, &buffer_list);
 * tiledb_buffer_list_free(&buffer_list);
 * @endcode
 *
 * @param buffer_list The buffer list to be destroyed.
 */
TILEDB_EXPORT void tiledb_buffer_list_free(tiledb_buffer_list_t** buffer_list)
    TILEDB_NOEXCEPT;

/**
 * Gets the number of buffers in the buffer list.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_buffer_list_t* buffer_list;
 * tiledb_buffer_list_alloc(ctx, &buffer_list);
 * uint64_t num_buffers;
 * tiledb_buffer_list_get_num_buffers(ctx, buffer_list, &num_buffers);
 * // num_buffers == 0 because the list is empty.
 * @endcode
 *
 * @param ctx TileDB context.
 * @param buffer_list The buffer list.
 * @param num_buffers Set to the number of buffers in the buffer list.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_buffer_list_get_num_buffers(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_list_t* buffer_list,
    uint64_t* num_buffers) TILEDB_NOEXCEPT;

/**
 * Gets the buffer at the given index in the buffer list. The returned buffer
 * object is simply a pointer to memory managed by the underlying buffer
 * list, meaning this function does not perform a copy.
 *
 * It is the caller's responsibility to free the returned buffer with
 * `tiledb_buffer_free`. Since the returned buffer object does not "own" the
 * underlying allocation, the underlying allocation is not freed when freeing it
 * with `tiledb_buffer_free`.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_buffer_list_t* buffer_list;
 * // Create and populate the buffer_list
 *
 * // Get the buffer at index 0.
 * tiledb_buffer_t *buff0;
 * tiledb_buffer_list_get_buffer(ctx, buffer_list, 0, &buff0);
 *
 * // Always free the returned buffer object
 * tiledb_buffer_free(&buff0);
 * tiledb_buffer_list_free(&buffer_list);
 * @endcode
 *
 * @param ctx TileDB context.
 * @param buffer_list The buffer list.
 * @param buffer_idx Index of buffer to get from the buffer list.
 * @param buffer Set to a newly allocated buffer object pointing to the
 *    underlying allocation in the buffer list corresponding to the buffer.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_buffer_list_get_buffer(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_list_t* buffer_list,
    uint64_t buffer_idx,
    tiledb_buffer_t** buffer) TILEDB_NOEXCEPT;

/**
 * Gets the total number of bytes in the buffers in the buffer list.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_buffer_list_t* buffer_list;
 * tiledb_buffer_list_alloc(ctx, &buffer_list);
 * uint64_t total_size;
 * tiledb_buffer_list_get_total_size(ctx, buffer_list, &total_size);
 * // total_size == 0 because the list is empty.
 * @endcode
 *
 * @param ctx TileDB context.
 * @param buffer_list The buffer list.
 * @param total_size Set to the total number of bytes in the buffers in the
 *    buffer list.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_buffer_list_get_total_size(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_list_t* buffer_list,
    uint64_t* total_size) TILEDB_NOEXCEPT;

/**
 * Copies and concatenates all the data in the buffer list into a new buffer.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_buffer_t* buff;
 * tiledb_buffer_list_flatten(ctx, buffer_list, &buff);
 * // ...
 * tiledb_buffer_free(&buff);
 * @endcode
 *
 * @param ctx TileDB context.
 * @param buffer_list The buffer list.
 * @param buffer Will be set to a newly allocated buffer holding a copy of the
 *    concatenated data from the buffer list.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_buffer_list_flatten(
    tiledb_ctx_t* ctx,
    tiledb_buffer_list_t* buffer_list,
    tiledb_buffer_t** buffer) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_BUFFER_LIST_API_EXTERNAL_H
