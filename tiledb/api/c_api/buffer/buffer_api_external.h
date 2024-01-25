/**
 * @file tiledb/api/c_api/buffer/buffer_api_external.h
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
 * This file declares the buffer C API for TileDB.
 */

#ifndef TILEDB_CAPI_BUFFER_API_EXTERNAL_H
#define TILEDB_CAPI_BUFFER_API_EXTERNAL_H

#include "../api_external_common.h"
#include "../context/context_api_external.h"
#include "../datatype/datatype_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/** A buffer object. */
typedef struct tiledb_buffer_handle_t tiledb_buffer_t;

/**
 * Creates an empty buffer object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_buffer_t* buffer;
 * tiledb_buffer_alloc(ctx, &buffer);
 * @endcode
 *
 * @param ctx TileDB context
 * @param buffer The buffer to be created
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_buffer_alloc(
    tiledb_ctx_t* ctx, tiledb_buffer_t** buffer) TILEDB_NOEXCEPT;

/**
 * Destroys a TileDB buffer, freeing associated memory.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_buffer_t* buffer;
 * tiledb_buffer_alloc(ctx, &buffer);
 * tiledb_buffer_free(&buffer);
 * @endcode
 *
 * @param buffer The buffer to be destroyed.
 */
TILEDB_EXPORT void tiledb_buffer_free(tiledb_buffer_t** buffer) TILEDB_NOEXCEPT;

/**
 * Sets a datatype for the given buffer. The default datatype is `TILEDB_UINT8`.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_buffer_t* buffer;
 * tiledb_buffer_alloc(ctx, &buffer);
 * tiledb_buffer_set_type(ctx, buffer, TILEDB_INT32);
 * @endcode
 *
 * @param ctx TileDB context
 * @param buffer TileDB buffer instance
 * @param datatype The datatype to set on the buffer.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_buffer_set_type(
    tiledb_ctx_t* ctx,
    tiledb_buffer_t* buffer,
    tiledb_datatype_t datatype) TILEDB_NOEXCEPT;

/**
 * Gets the datatype from the given buffer.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_datatype_t type;
 * tiledb_buffer_get_type(ctx, buffer, &type);
 * @endcode
 *
 * @param ctx TileDB context
 * @param buffer TileDB buffer instance
 * @param datatype Set to the datatype of the buffer.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_buffer_get_type(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_datatype_t* datatype) TILEDB_NOEXCEPT;

/**
 * Gets a pointer to the current allocation and the current number of bytes in
 * the specified buffer object.
 *
 * @note For string buffers allocated by TileDB, the number of bytes includes
 * the terminating NULL byte.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_buffer_t* buffer;
 * tiledb_buffer_alloc(ctx, &buffer);
 * void* data;
 * uint64_t num_bytes;
 * tiledb_buffer_get_data(ctx, buffer, &data, num_bytes);
 * // data == NULL and num_bytes == 0 because the buffer is currently empty.
 * tiledb_buffer_free(&buffer);
 * @endcode
 *
 * @param ctx TileDB context
 * @param buffer TileDB buffer instance
 * @param data The pointer to the data to be retrieved.
 * @param num_bytes Set to the number of bytes in the buffer.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_buffer_get_data(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    void** data,
    uint64_t* num_bytes) TILEDB_NOEXCEPT;

/**
 * Sets (wraps) a pre-allocated region of memory with the given buffer object.
 * This does not perform a copy.
 *
 * @note The TileDB buffer object does not take ownership of the allocation
 * set with this function. That means the call to `tiledb_buffer_free` will not
 * free a user allocation set via `tiledb_buffer_set_buffer`.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_buffer_t* buffer;
 * tiledb_buffer_alloc(ctx, &buffer);
 *
 * void* my_data = malloc(100);
 * tiledb_buffer_set_data(ctx, buffer, my_data, 100);
 *
 * void* data;
 * uint64_t num_bytes;
 * tiledb_buffer_get_data(ctx, buffer, &data, num_bytes);
 * assert(data == my_data);
 * assert(num_bytes == 100);
 *
 * tiledb_buffer_free(&buffer);
 * free(my_data);
 * @endcode
 *
 * @param ctx TileDB context
 * @param buffer TileDB buffer instance
 * @param data Pre-allocated region of memory to wrap with this buffer.
 * @param size Size (in bytes) of the region pointed to by data.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_buffer_set_data(
    tiledb_ctx_t* ctx, tiledb_buffer_t* buffer, void* data, uint64_t size)
    TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_BUFFER_API_EXTERNAL_H
