/**
 * @file tiledb/api/c_api/subarray/subarray_api_external.h
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
 * This file declares the Subarray C API for TileDB.
 */

#ifndef TILEDB_CAPI_SUBARRAY_EXTERNAL_H
#define TILEDB_CAPI_SUBARRAY_EXTERNAL_H

#include "../api_external_common.h"
#include "../array/array_api_external.h"
#include "../context/context_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/** C API carrier for a TileDB subarray object. */
typedef struct tiledb_subarray_handle_t tiledb_subarray_t;

/**
 * Allocates a TileDB subarray object.
 *
 * @note The allocated subarray initially has internal coalesce_ranges == true.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_subarray_t* subarray;
 * tiledb_subarray_alloc(ctx, array, &subarray);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array An open array object.
 * @param[out] subarray The subarray object to be created.
 * @return `TILEDB_OK` for success or `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_alloc(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_subarray_t** subarray) TILEDB_NOEXCEPT;

/**
 * Frees a TileDB subarray object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_subarray_t* subarray;
 * tiledb_array_open(ctx, array, TILEDB_READ);
 * tiledb_subarray_alloc(ctx, array, &subarray);
 * tiledb_array_close(ctx, array);
 * tiledb_subarray_free(&subarray);
 * @endcode
 *
 * @param[in] subarray The subarray object to be freed.
 */
TILEDB_EXPORT void tiledb_subarray_free(tiledb_subarray_t** subarray)
    TILEDB_NOEXCEPT;

/**
 * Set the subarray config.
 *
 * @note This function _only_ overrides config parameter `sm.read_range_oob`.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_subarray_t* subarray;
 * tiledb_subarray_alloc(ctx, array, &subarray);
 * tiledb_config_t config;
 * tiledb_subarray_set_config(ctx, subarray, &config);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] subarray The subarray object to set the config on.
 * @param[in] config The config to set on the subarray.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_set_config(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    tiledb_config_t* config) TILEDB_NOEXCEPT;

/**
 * Sets coalesce_ranges property on a TileDB subarray object.
 * Intended to be used just after tiledb_subarray_alloc() to replace
 * the initial coalesce_ranges == true with coalesce_ranges = false if needed.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_subarray_t* subarray;
 * //tiledb_subarray_alloc internally defaults to 'coalesce_ranges == true'
 * tiledb_subarray_alloc(ctx, array, &subarray);
 * // so manually set to 'false' to match earlier behaviour with older
 * // tiledb_query_ subarray actions.
 * bool coalesce_ranges = false;
 * tiledb_subarray_set_coalesce_ranges(ctx, subarray, coalesce_ranges);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] subarray The subarray object to change.
 * @param[in] coalesce_ranges The true/false value to be set
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_set_coalesce_ranges(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    int coalesce_ranges) TILEDB_NOEXCEPT;

/**
 * Populates a subarray with specific indicies.
 *
 * **Example:**
 *
 * The following sets a 2D subarray [0,10], [20, 30] on the subarray.
 *
 * @code{.c}
 * tiledb_subarray_t *subarray;
 * uint64_t subarray_v[] = { 0, 10, 20, 30};
 * tiledb_subarray_set_subarray(ctx, subarray, subarray_v);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] subarray The TileDB subarray object.
 * @param[in] subarray_v The subarray values which can be used to limit the
 *  subarray read/write.
 *     It should be a sequence of [low, high] pairs (one pair per dimension).
 *     When the subarray is used for writes, this is meaningful only for dense
 *     arrays, and specifically dense writes. Note that `subarray_a` must have
 *     the same type as the domain of the subarray's associated array.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_set_subarray(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const void* subarray_v) TILEDB_NOEXCEPT;

/**
 * Adds a 1D range along a subarray dimension index, which is in the form
 * (start, end, stride). The datatype of the range components must be the same
 * as the type of the domain of the array in the query.
 *
 * @note The stride is currently unsupported. Use 0/NULL/nullptr as the
 * stride argument.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t dim_idx = 2;
 * int64_t start = 10;
 * int64_t end = 20;
 * tiledb_subarray_add_range(ctx, subarray, dim_idx, &start, &end, nullptr);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] subarray The subarray to add the range to.
 * @param[in] dim_idx The index of the dimension to add the range to.
 * @param[in] start The range start.
 * @param[in] end The range end.
 * @param[in] stride The range stride.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_add_range(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const void* start,
    const void* end,
    const void* stride) TILEDB_NOEXCEPT;

/**
 * Adds a 1D range along a subarray dimension name, which is in the form
 * (start, end, stride). The datatype of the range components must be the same
 * as the type of the domain of the array in the query.
 *
 * @note The stride is currently unsupported. Use 0/NULL/nullptr as the
 * stride argument.
 *
 * **Example:**
 *
 * @code{.c}
 * char* dim_name = "rows";
 * int64_t start = 10;
 * int64_t end = 20;
 * tiledb_subarray_add_range_by_name(
 *     ctx, subarray, dim_name, &start, &end, nullptr);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] subarray The subarray to add the range to.
 * @param[in] dim_name The name of the dimension to add the range to.
 * @param[in] start The range start.
 * @param[in] end The range end.
 * @param[in] stride The range stride.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_add_range_by_name(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* dim_name,
    const void* start,
    const void* end,
    const void* stride) TILEDB_NOEXCEPT;

/**
 * Adds a 1D variable-sized range along a subarray dimension index, which is in
 * the form (start, end). Applicable only to variable-sized dimensions.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t dim_idx = 2;
 * char start[] = "a";
 * char end[] = "bb";
 * tiledb_subarray_add_range_var(ctx, subarray, dim_idx, start, 1, end, 2);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] subarray The subarray to add the range to.
 * @param[in] dim_idx The index of the dimension to add the range to.
 * @param[in] start The range start.
 * @param[in] start_size The size of the range start in bytes.
 * @param[in] end The range end.
 * @param[in] end_size The size of the range end in bytes.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_add_range_var(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) TILEDB_NOEXCEPT;

/**
 * Adds a 1D variable-sized range along a subarray dimension name, which is in
 * the form (start, end). Applicable only to variable-sized dimensions.
 *
 * **Example:**
 *
 * @code{.c}
 * char* dim_name = "rows";
 * char start[] = "a";
 * char end[] = "bb";
 * tiledb_subarray_add_range_var_by_name(
 *     ctx, subarray, dim_name, start, 1, end, 2);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] subarray The subarray to add the range to.
 * @param[in] dim_name The name of the dimension to add the range to.
 * @param[in] start The range start.
 * @param[in] start_size The size of the range start in bytes.
 * @param[in] end The range end.
 * @param[in] end_size The size of the range end in bytes.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_add_range_var_by_name(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* dim_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) TILEDB_NOEXCEPT;

/**
 * Retrieves the number of ranges of the query subarray along a given dimension
 * index.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t range_num;
 * tiledb_subarray_get_range_num(ctx, subarray, dim_idx, &range_num);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] subarray The subarray.
 * @param[in] dim_idx The index of the dimension for which to retrieve number of
 * ranges.
 * @param[out] range_num The retrieved number of ranges.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_get_range_num(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    uint64_t* range_num) TILEDB_NOEXCEPT;

/**
 * Retrieve the number of ranges of the subarray along a given dimension name.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t range_num;
 * tiledb_subarray_get_range_num_from_name(ctx, subarray, dim_name, &range_num);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] subarray The subarray.
 * @param[in] dim_name The name of the dimension whose range number to retrieve.
 * @param[out] range_num The retrieved number of ranges.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_get_range_num_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t* range_num) TILEDB_NOEXCEPT;

/**
 * Retrieves a specific range of the subarray along a given dimension index.
 *
 * **Example:**
 *
 * @code{.c}
 * const void* start;
 * const void* end;
 * const void* stride;
 * tiledb_subarray_get_range(
 *     ctx, subarray, dim_idx, range_idx, &start, &end, &stride);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] subarray The subarray.
 * @param[in] dim_idx The index of the dimension to retrieve the range from.
 * @param[in] range_idx The index of the range to retrieve.
 * @param[out] start The retrieved range start.
 * @param[out] end The received range end.
 * @param[out] stride The retrieved range stride.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_get_range(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) TILEDB_NOEXCEPT;

/**
 * Retrieves a specific range of the subarray along a given dimension name.
 *
 * **Example:**
 *
 * @code{.c}
 * const void* start;
 * const void* end;
 * const void* stride;
 * tiledb_subarray_get_range_from_name(
 *     ctx, query, dim_name, range_idx, &start, &end, &stride);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] subarray The subarray.
 * @param[in] dim_name The name of the dimension to retrieve the range from.
 * @param[in] range_idx The index of the range to retrieve.
 * @param[out] start The retrieved range start.
 * @param[out] end The retrieved range end.
 * @param[out] stride The retrieved range stride.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_get_range_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) TILEDB_NOEXCEPT;

/**
 * Retrieves a range's start and end size for a given variable-length
 * dimension index at a given range index.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t start_size;
 * uint64_t end_size;
 * tiledb_subarray_get_range_var_size(
 *     ctx, subarray, dim_idx, range_idx, &start_size, &end_size);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] subarray The subarray.
 * @param[in] dim_idx The index of the dimension to retrieve the range from.
 * @param[in] range_idx The index of the range to retrieve.
 * @param[out] start_size The retrieved range start size in bytes
 * @param[out] end_size The retrieved range end size in bytes
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_get_range_var_size(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) TILEDB_NOEXCEPT;

/**
 * Retrieves a range's start and end size for a given variable-length
 * dimension name at a given range index.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t start_size;
 * uint64_t end_size;
 * tiledb_subarray_get_range_var_size_from_name(
 *     ctx, subarray, dim_name, range_idx, &start_size, &end_size);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] subarray The subarray.
 * @param[in] dim_name The name of the dimension to retrieve the range from.
 * @param[in] range_idx The index of the range to retrieve.
 * @param[out] start_size The retrieved range start size in bytes
 * @param[out] end_size The retrieved range end size in bytes
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_get_range_var_size_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) TILEDB_NOEXCEPT;

/**
 * Retrieves a specific range of the subarray along a given
 * variable-length dimension index.
 *
 * **Example:**
 *
 * @code{.c}
 * const void* start;
 * const void* end;
 * tiledb_subarray_get_range_var(
 *     ctx, subarray, dim_idx, range_idx, &start, &end);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] subarray The subarray.
 * @param[in] dim_idx The index of the dimension to retrieve the range from.
 * @param[in] range_idx The index of the range to retrieve.
 * @param[out] start The retrieved range start.
 * @param[out] end The retrieved range end.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_get_range_var(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    uint64_t range_idx,
    void* start,
    void* end) TILEDB_NOEXCEPT;

/**
 * Retrieves a specific range of the subarray along a given
 * variable-length dimension name.
 *
 * **Example:**
 *
 * @code{.c}
 * const void* start;
 * const void* end;
 * tiledb_subarray_get_range_var_from_name(
 *     ctx, subarray, dim_name, range_idx, &start, &end);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] subarray The subarray.
 * @param[in] dim_name The name of the dimension to retrieve the range from.
 * @param[in] range_idx The index of the range to retrieve.
 * @param[out] start The retrieved range start.
 * @param[out] end The retrieved range end.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_get_range_var_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    void* start,
    void* end) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_SUBARRAY_EXTERNAL_H
