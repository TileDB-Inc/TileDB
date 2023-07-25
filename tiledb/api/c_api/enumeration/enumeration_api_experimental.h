/**
 * @file tiledb/api/c_api/enumeration/enumeration_api_experimental.h
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
 * This file declares the enumeration section of the C API for TileDB.
 */

#ifndef TILEDB_CAPI_ENUMERATION_EXPERIMENTAL_H
#define TILEDB_CAPI_ENUMERATION_EXPERIMENTAL_H

#include <stdio.h>

#include "../api_external_common.h"
#include "../context/context_api_external.h"
#include "../datatype/datatype_api_external.h"
#include "../string/string_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/** A TileDB dimension. */
typedef struct tiledb_enumeration_handle_t tiledb_enumeration_t;

/**
 * Creates an Enumeration.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_enumeration_t* enumeration;
 * void* data = get_data();
 * uint64_t data_size = get_data_size();
 * tiledb_enumeration_alloc(
 *     ctx,
 *     TILEDB_INT64,
 *     cell_val_num,
 *     FALSE,
 *     data,
 *     data_size,
 *     nullptr,
 *     0,
 *     &enumeration);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param name The name of the enumeration.
 * @param type The enumeration type.
 * @param cell_val_num The number of values per enumeration value.
 * @param ordered Whether this enumeration should be considered as ordered.
 * @param data A pointer to the enumeration value data.
 * @param data_size The length of the data buffer provided.
 * @param offsets A pointer to the offsets buffer if cell_vall_num
 *        is TILEDB_VAR_NUM.
 * @param offsets_size The length of the offsets buffer, zero if no offsets.
 * @param enumeration The newly allocated enumeration.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_enumeration_alloc(
    tiledb_ctx_t* ctx,
    const char* name,
    tiledb_datatype_t type,
    uint32_t cell_val_num,
    int ordered,
    const void* data,
    uint64_t data_size,
    const void* offsets,
    uint64_t offsets_size,
    tiledb_enumeration_t** enumeration) TILEDB_NOEXCEPT;

/**
 * Destroys a TileDB enumeration, freeing associated memory.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_enumeration_free(&enumeration);
 * @endcode
 *
 * @param enumeration The enumeration to be destroyed.
 */
TILEDB_EXPORT void tiledb_enumeration_free(tiledb_enumeration_t** enumeration)
    TILEDB_NOEXCEPT;

/**
 * Return the datatype of the enumeration values
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_string_t* name_str;
 * tiledb_enumeration_get_type(ctx, enumeration, &name_str);
 * const char* name;
 * size_t name_len;
 * tiledb_string_view(name_str, &name, &name_len);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param enumeration The enumeration.
 * @param name The name of the enumeration.
 * @return `TILEDB_OK` or `TILEDB_ERR`.
 */
TILEDB_EXPORT capi_return_t tiledb_enumeration_get_name(
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    tiledb_string_t** name) TILEDB_NOEXCEPT;

/**
 * Return the datatype of the enumeration values
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_datatype_t type;
 * tiledb_enumeration_get_type(ctx, enumeration, &type);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param enumeration The enumeration.
 * @param type The data type of the enumeration.
 * @return `TILEDB_OK` or `TILEDB_ERR`.
 */
TILEDB_EXPORT capi_return_t tiledb_enumeration_get_type(
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    tiledb_datatype_t* type) TILEDB_NOEXCEPT;

/**
 * Return the cell value number of the enumeration values
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t cell_val_num;
 * tiledb_enumeration_get_cell_val_num(ctx, enumeration, &cell_val_num);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param enumeration The enumeration.
 * @param type The cell value number of the enumeration.
 * @return `TILEDB_OK` or `TILEDB_ERR`.
 */
TILEDB_EXPORT capi_return_t tiledb_enumeration_get_cell_val_num(
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    uint32_t* cell_val_num) TILEDB_NOEXCEPT;

/**
 * Return whether the enumeration values should be considered ordered.
 *
 * **Example:**
 *
 * @code{.c}
 * int ordered;
 * tiledb_enumeration_get_ordered(ctx, enumeration, &ordered);
 * @endcode
 *
 * The cell values are considered if the value in ordered after `TILEDB_OK`
 * is returned is non-zero. I.e., this is standard `int` as `bool` behavior.
 *
 * @param ctx The TileDB context.
 * @param enumeration The enumeration.
 * @param ordered A boolean value indicating whether the values are ordered.
 * @return `TILEDB_OK` or `TILEDB_ERR`.
 */
TILEDB_EXPORT capi_return_t tiledb_enumeration_get_ordered(
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    int* ordered) TILEDB_NOEXCEPT;

/**
 * Return a pointer to and size of the enumerations underlying value data
 *
 * **Example:**
 *
 * @code{.c}
 * void* data = NULL;
 * uint64_t data_size = 0;
 * tiledb_enumeration_get_data(ctx, enumeration, &data, &data_size);
 * @endcode
 *
 * The pointer returned from this function references internal data managed
 * by the enumeration. As such, clients should not attempt to free it, or
 * access it beyond the lifetime of the enumeration instance.
 *
 * @param ctx The TileDB context.
 * @param enumeration The enumeration.
 * @param data The returned pointer to this enumeration value buffer.
 * @param data_size The length of the buffer pointed to by data.
 * @return `TILEDB_OK` or `TILEDB_ERR`.
 */
TILEDB_EXPORT capi_return_t tiledb_enumeration_get_data(
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    const void** data,
    uint64_t* data_size) TILEDB_NOEXCEPT;

/**
 * Return a pointer to and size of the enumerations underlying value offsets
 *
 * **Example:**
 *
 * @code{.c}
 * void* offsets = NULL;
 * uint64_t offsets_size = 0;
 * tiledb_enumeration_get_offsets(ctx, enumeration, &offsets, &offsets_size);
 * @endcode
 *
 * The pointer returned from this function references internal data managed
 * by the enumeration. As such, clients should not attempt to free it, or
 * access it beyond the lifetime of the enumeration instance.
 *
 * If the enumeration values are var sized (i.e., cell_var_num is
 * TILEDB_VAR_NUM) the offsets buffer will contain a `uint64_t` value for the
 * starting offset of each underlying enumeration value. Note that the number
 * of offsets is calculated as `offsets_size / sizeof(uint64_t)`.
 *
 * @param ctx The TileDB context.
 * @param enumeration The enumeration.
 * @param data The returned pointer to this enumeration offsets buffer.
 * @param data_size The length of the buffer pointed to by offsets.
 * @return `TILEDB_OK` or `TILEDB_ERR`.
 */
TILEDB_EXPORT capi_return_t tiledb_enumeration_get_offsets(
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    const void** offsets,
    uint64_t* offsets_size) TILEDB_NOEXCEPT;

/**
 * Dumps the contents of an Enumeration in ASCII form to some output (e.g.,
 * file or stdout).
 *
 * **Example:**
 *
 * The following prints the enumeration dump to standard output.
 *
 * @code{.c}
 * tiledb_enumeration_dump(ctx, enmr, stdout);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The attribute.
 * @param out The output.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error./
 */
TILEDB_EXPORT capi_return_t tiledb_enumeration_dump(
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    FILE* out) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_ENUMERATION_EXPERIMENTAL_H
