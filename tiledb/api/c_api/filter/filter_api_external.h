/**
 * @file tiledb/api/c_api/filter/filter_api_external.h
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

#ifndef TILEDB_CAPI_FILTER_EXTERNAL_H
#define TILEDB_CAPI_FILTER_EXTERNAL_H

#include "../api_external_common.h"
#include "../context/context_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * C API carrier for a TileDB filter
 */
typedef struct tiledb_filter_handle_t tiledb_filter_t;

/**
 * Filter type.
 */
typedef enum {
/** Selection macro for defining filter type enums. */
#define TILEDB_FILTER_TYPE_ENUM(id) TILEDB_##id
#include "filter_api_enum.h"
#undef TILEDB_FILTER_TYPE_ENUM
} tiledb_filter_type_t;

/**
 * Filter option.
 */
typedef enum {
/** Selection macro for defining filter option enums. */
#define TILEDB_FILTER_OPTION_ENUM(id) TILEDB_##id
#include "filter_api_enum.h"
#undef TILEDB_FILTER_OPTION_ENUM
} tiledb_filter_option_t;

/**
 * WebP filter input format enum.
 */
typedef enum {
/** Selection macro for defining webp filter format type enum. */
#define TILEDB_FILTER_WEBP_FORMAT(id) TILEDB_##id
#include "filter_api_enum.h"
#undef TILEDB_FILTER_WEBP_FORMAT
} tiledb_filter_webp_format_t;

/**
 * Returns a string representation of the given filter type.
 *
 * @param[in] filter_type Filter type
 * @param[out] str Set to point to a constant string representation of the
 * filter type
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filter_type_to_str(
    tiledb_filter_type_t filter_type, const char** str) TILEDB_NOEXCEPT;

/**
 * Parses a filter type from the given string.
 *
 * @param[in] str String representation to parse
 * @param[out] filter_type Set to the parsed filter type
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filter_type_from_str(
    const char* str, tiledb_filter_type_t* filter_type) TILEDB_NOEXCEPT;

/**
 * Returns a string representation of the given filter option.
 *
 * @param[in] filter_option Filter option
 * @param[out] str Set to point to a constant string representation of the
 * filter option
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filter_option_to_str(
    tiledb_filter_option_t filter_option, const char** str) TILEDB_NOEXCEPT;

/**
 * Parses a filter option from the given string.
 *
 * @param[in] str String representation to parse
 * @param[out] filter_option Set to the parsed filter option
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filter_option_from_str(
    const char* str, tiledb_filter_option_t* filter_option) TILEDB_NOEXCEPT;

/**
 * Creates a TileDB filter.
 *
 * The filter returned has independent lifespan. It will be available until
 * tiledb_filter_free is called on it.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_t* filter;
 * tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] type The filter type.
 * @param[out] filter The TileDB filter to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filter_alloc(
    tiledb_ctx_t* ctx,
    tiledb_filter_type_t type,
    tiledb_filter_t** filter) TILEDB_NOEXCEPT;

/**
 * Destroys a TileDB filter, freeing associated memory.
 *
 * This function must be called on every filter returned from the API, whether
 * they have independent or subordinate lifespans.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_t* filter;
 * tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter);
 * tiledb_filter_free(&filter);
 * @endcode
 *
 * @param[in,out] filter The filter to be destroyed.
 */
TILEDB_EXPORT void tiledb_filter_free(tiledb_filter_t** filter) TILEDB_NOEXCEPT;

/**
 * Retrieves the type of a filter.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_t* filter;
 * tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter);
 * tiledb_filter_type_t type;
 * tiledb_filter_get_type(ctx, filter, &type);
 * // type == TILEDB_FILTER_BZIP2
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] filter The TileDB filter.
 * @param[out] type The filter type to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filter_get_type(
    tiledb_ctx_t* ctx,
    tiledb_filter_t* filter,
    tiledb_filter_type_t* type) TILEDB_NOEXCEPT;

/**
 * Sets an option on a filter. Options are filter dependent; this function
 * returns an error if the given option is not valid for the given filter.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_t* filter;
 * tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter);
 * int32_t level = 5;
 * tiledb_filter_set_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
 * tiledb_filter_free(&filter);
 * @endcode
 *
 * @param[in] ctx TileDB context.
 * @param[in] filter The target filter.
 * @param[in] option Filter option to set.
 * @param[out] value Value of option to set.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filter_set_option(
    tiledb_ctx_t* ctx,
    tiledb_filter_t* filter,
    tiledb_filter_option_t option,
    const void* value) TILEDB_NOEXCEPT;

/**
 * Gets an option value from a filter. Options are filter dependent; this
 * function returns an error if the given option is not valid for the given
 * filter.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_t* filter;
 * tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter);
 * int32_t level;
 * tiledb_filter_get_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
 * // level == -1 (the default)
 * tiledb_filter_free(&filter);
 * @endcode
 *
 * @note The buffer pointed to by `value` must be large enough to hold the
 * option value.
 *
 * @param[in] ctx TileDB context.
 * @param[in] filter The target filter.
 * @param[in] option Filter option to get.
 * @param[out] value Buffer that option value will be written to.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filter_get_option(
    tiledb_ctx_t* ctx,
    tiledb_filter_t* filter,
    tiledb_filter_option_t option,
    void* value) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_FILTER_EXTERNAL_H
