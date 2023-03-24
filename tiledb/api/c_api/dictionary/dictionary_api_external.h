/**
 * @file tiledb/api/c_api/dictionary/dictionary_api_external.h
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
 * C API Wrapper around the Dictionary class.
 */

#ifndef TILEDB_CAPI_DICTIONARY_EXTERNAL_H
#define TILEDB_CAPI_DICTIONARY_EXTERNAL_H

// For the `FILE *` argument in `tiledb_dictionary_dump`
#include <stdio.h>

#include "../api_external_common.h"
#include "../context/context_api_external.h"
#include "../datatype/datatype_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/** A TileDB dictionary. */
typedef struct tiledb_dictionary_handle_t tiledb_dictionary_t;

/**
 * Creates a dictionary.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_dictionary_t* dict;
 * tiledb_dictionary_alloc(
 *     ctx, TILEDB_STRING_UTF8, &dict);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param type The dictionary type.
 * @param dict The dictionary to be created.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_dictionary_alloc(
    tiledb_ctx_t* ctx,
    tiledb_datatype_t type,
    tiledb_dictionary_t** dict) TILEDB_NOEXCEPT;

/**
 * Destroys a TileDB dictionary, freeing associated memory.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_dictionary_free(&dict);
 * @endcode
 *
 * @param dict The dictionary to be destroyed.
 */
TILEDB_EXPORT void tiledb_dictionary_free(tiledb_dictionary_t** dict)
    TILEDB_NOEXCEPT;

/**
 * Retrieves the dictionary type.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_datatype_t dict_type;
 * tiledb_dictionary_get_type(ctx, dict, &dict_type);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The dictionary.
 * @param type The type to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_dictionary_get_type(
    tiledb_ctx_t* ctx,
    const tiledb_dictionary_t* dict,
    tiledb_datatype_t* type) TILEDB_NOEXCEPT;

/**
 * Sets the number of values per cell for a dictionary. If this is not
 * used, the default is `1`.
 *
 * **Examples:**
 *
 * For a fixed-sized dictionary:
 *
 * @code{.c}
 * tiledb_dictionary_set_cell_val_num(ctx, dict, 3);
 * @endcode
 *
 * For a variable-sized dictionary:
 *
 * @code{.c}
 * tiledb_dictionary_set_cell_val_num(ctx, dict, TILEDB_VAR_NUM);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dict The target dictionary.
 * @param cell_val_num The number of values per cell.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_attribute_set_cell_val_num(
    tiledb_ctx_t* ctx,
    tiledb_dictionary_t* dict,
    uint32_t cell_val_num) TILEDB_NOEXCEPT;

/**
 * Retrieves the number of values per cell for the dictinary. For variable-sized
 * attributes result is TILEDB_VAR_NUM.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t num;
 * tiledb_dictionary_get_cell_val_num(ctx, dict, &num);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dict The dictionary.
 * @param cell_val_num The number of values per cell to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_dictionary_get_cell_val_num(
    tiledb_ctx_t* ctx,
    const tiledb_dictionary_t* dict,
    uint32_t* cell_val_num) TILEDB_NOEXCEPT;

/**
 * Sets the nullability of a dictionary.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_dictionary_set_nullable(ctx, dict, 1);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dict The target dictionary.
 * @param nullable Non-zero if the dictionary is nullable.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_attribute_set_nullable(
    tiledb_ctx_t* ctx,
    tiledb_dictionary_t* dict,
    uint8_t nullable) TILEDB_NOEXCEPT;

/**
 * Gets the nullability of a dictionary.
 *
 * **Example:**
 *
 * @code{.c}
 * uint8_t nullable;
 * tiledb_dictionary_get_nullable(ctx, dict, &nullable);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dict The target dictionary.
 * @param nullable The nullability status to be retrieved
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_attribute_get_nullable(
    tiledb_ctx_t* ctx,
    tiledb_dictionary_t* dict,
    uint8_t* nullable) TILEDB_NOEXCEPT;

/**
 * Sets whether the dictionary values are ordered.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_dictionary_set_ordered(ctx, dict, 1);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dict The target dictionary.
 * @param ordered Non-zero if the dictionary is considered ordered.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_attribute_set_ordered(
    tiledb_ctx_t* ctx,
    tiledb_dictionary_t* dict,
    uint8_t nullable) TILEDB_NOEXCEPT;

/**
 * Gets whether dictionary values are ordered.
 *
 * **Example:**
 *
 * @code{.c}
 * uint8_t ordered;
 * tiledb_dictionary_get_ordered(ctx, dict, &nullable);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dict The target dictionary.
 * @param ordered The ordered status to be retrieved
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_attribute_get_ordered(
    tiledb_ctx_t* ctx,
    tiledb_dictionary_t* dict,
    uint8_t* ordered) TILEDB_NOEXCEPT;

/**
 * Sets the data buffer for the dictionary.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t values[100];
 * tiledb_dictionary_set_data_buffer(ctx, dict, values, sizeof(values));
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dict The TileDB dictionary.
 * @param buffer A buffer containing the values for the dictionary
 * @param buffer_size The length of buffer in bytes
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_dictionary_set_data_buffer(
    tiledb_ctx_t* ctx,
    tiledb_dictionary_t* dict,
    void* buffer,
    uint64_t buffer_size) TILEDB_NOEXCEPT;

/**
 * Gets the data buffer for the dictionary.
 *
 * **Example:**
 *
 * @code{.c}
 * void* buffer
 * uint64_t buffer_size;
 * tiledb_dictionary_get_data_buffer(ctx, dict, &buffer, &buffer_size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dict The TileDB dictionary.
 * @param buffer A buffer containing the values for the dictionary
 * @param buffer_size The length of buffer in bytes
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_dictionary_get_data_buffer(
    tiledb_ctx_t* ctx,
    tiledb_dictionary_t* dict,
    void** buffer,
    uint64_t* buffer_size) TILEDB_NOEXCEPT;

/**
 * Sets the offsets buffer for the dictionary for var sized types.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t offsets[100];
 * tiledb_dictionary_set_offsets_buffer(ctx, dict, offsets, sizeof(offsets));
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dict The TileDB dictionary.
 * @param buffer A buffer containing the offsets for the dictionary
 * @param buffer_size The length of buffer in bytes
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_dictionary_set_offsets_buffer(
    tiledb_ctx_t* ctx,
    tiledb_dictionary_t* dict,
    void* buffer,
    uint64_t buffer_size) TILEDB_NOEXCEPT;

/**
 * Gets the offsets buffer for the dictionary.
 *
 * **Example:**
 *
 * @code{.c}
 * void* buffer
 * uint64_t buffer_size;
 * tiledb_dictionary_get_offsets_buffer(ctx, dict, &buffer, &buffer_size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dict The TileDB dictionary.
 * @param buffer A buffer containing the offsets for the dictionary
 * @param buffer_size The length of buffer in bytes
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_dictionary_get_offsets_buffer(
    tiledb_ctx_t* ctx,
    tiledb_dictionary_t* dict,
    void** buffer,
    uint64_t* buffer_size) TILEDB_NOEXCEPT;

/**
 * Sets the validity buffer for the dictionary.
 *
 * **Example:**
 *
 * @code{.c}
 * uint8_t nulls[100];
 * tiledb_dictionary_set_validity_buffer(ctx, dict, nulls, sizeof(nulls));
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dict The TileDB dictionary.
 * @param buffer A buffer containing the validity values for the dictionary
 * @param buffer_size The length of buffer in bytes
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_dictionary_set_validity_buffer(
    tiledb_ctx_t* ctx,
    tiledb_dictionary_t* dict,
    void* buffer,
    uint64_t buffer_size) TILEDB_NOEXCEPT;

/**
 * Gets the validity buffer for the dictionary.
 *
 * **Example:**
 *
 * @code{.c}
 * void* buffer
 * uint64_t buffer_size;
 * tiledb_dictionary_get_data_buffer(ctx, dict, &buffer, &buffer_size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dict The TileDB dictionary.
 * @param buffer A buffer containing the validity values for the dictionary
 * @param buffer_size The length of buffer in bytes
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_dictionary_get_validity_buffer(
    tiledb_ctx_t* ctx,
    tiledb_dictionary_t* dict,
    void** buffer,
    uint64_t* buffer_size) TILEDB_NOEXCEPT;

/**
 * Dumps the contents of a dictionary in ASCII form to some output (e.g.,
 * file or stdout).
 *
 * **Example:**
 *
 * The following prints the dictionary dump to standard output.
 *
 * @code{.c}
 * tiledb_dictionary_dump(ctx, dict, stdout);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dict The dictionary.
 * @param out The output.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_dictionary_dump(
    tiledb_ctx_t* ctx,
    const tiledb_dictionary_t* dict,
    FILE* out) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_DICTIONARY_EXTERNAL_H
