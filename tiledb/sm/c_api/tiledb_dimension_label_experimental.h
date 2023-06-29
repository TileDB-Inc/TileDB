/**
 * @file tiledb/sm/c_api/tiledb_dimension_label_experimental.h
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
 * Experimental C-API for dimension labels.
 */

#ifndef TILEDB_DIMENSION_LABEL_EXPERIMENTAL_H
#define TILEDB_DIMENSION_LABEL_EXPERIMENTAL_H

#include "tiledb.h"
#include "tiledb/api/c_api/dimension_label/dimension_label_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Adds a dimension label to a array schema.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_add_dimension_label(
 *     ctx,
 *     array_schema,
 *     0,
 *     "label",
 *     TILEDB_INCREASING_DATA,
 *     TILEDB_FLOAT64);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param dim_index The index of the dimension the labels are applied to.
 * @param name The name of the dimension label.
 * @param label_order The ordering/sort of the label data.
 * @param label_type The datatype of the label data.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_add_dimension_label(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const uint32_t dim_index,
    const char* name,
    tiledb_data_order_t label_order,
    tiledb_datatype_t label_type) TILEDB_NOEXCEPT;

/**
 * Retrieves a dimension label from an array schema with the requested name.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_t* array_schema;
 * tiledb_array_schema_load(ctx, "s3://tiledb_bucket/my_array", &array_schema);
 * tiledb_dimension_label_t* dimension_label;
 * tiledb_array_schema_get_dimension_label_from_name(
 *      ctx, array_schema, "label1", &dimension_label);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema The schema the dimension label is defined on.
 * @param[in] label_name The name of the dimension label.
 * @param[out] dim_label The target dimension label.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_get_dimension_label_from_name(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const char* label_name,
    tiledb_dimension_label_t** dim_label) TILEDB_NOEXCEPT;

/**
 * Checks whether the array schema has a dimension label of the given name.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t has_dim_label;
 * tiledb_array_schema_has_dimension_label(
 *     ctx, array_schema, "label_0", &has_dim_label);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param name The name of the dimension label to check for.
 * @param has_dim_label Set to `1` if the array schema has an attribute of the
 *      given name, else `0`.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_has_dimension_label(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char* name,
    int32_t* has_dim_label) TILEDB_NOEXCEPT;

/**
 * Sets a filter on a dimension label filter in an array schema.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_list_t* filter_list;
 * tiledb_filter_list_alloc(ctx, &filter_list);
 * tiledb_filter_list_add_filter(ctx, filter_list, filter);
 * tiledb_array_schema_set_dimension_label_filter_list(
 *    ctx,
 *    array_schema,
 *    "label",
 *    filter_list);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param label_name The dimension label name.
 * @param filter_list The filter_list to be set.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_set_dimension_label_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const char* label_name,
    tiledb_filter_list_t* filter_list) TILEDB_NOEXCEPT;

/**
 * Sets the tile extent on a dimension label in an array schema.
 *
 * **Example:**
 *
 * @code{.c}
 * int64_t tile_extent = 16;
 * tiledb_array_schema_add_dimension_label(
 *     ctx,
 *     array_schema,
 *     0,
 *     "label",
 *     TILEDB_INCREASING_DATA,
 *     TILEDB_FLOAT64);
 * tiledb_array_schema_set_dimension_label_tile_extent(
 *     ctx, array_schema, "label", TILEDB_INT64, &tile_extent);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param label_name The dimension label name.
 * @param type The type of the dimension the tile extent is being set on.
 * @param tile_extent The tile extent for the dimension of the dimension label.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note The dimension label tile extent must be the same datatype as the
 *     dimension it is set on, not as the label.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_set_dimension_label_tile_extent(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const char* label_name,
    tiledb_datatype_t type,
    const void* tile_extent) TILEDB_NOEXCEPT;

/**
 * Adds a 1D range along a subarray for a dimension label, which is in the form
 * (start, end, stride). The datatype of the range components must be the same
 * as the datatype of label.
 *
 * **Example:**
 *
 * @code{.c}
 * int64_t start = 10;
 * int64_t end = 20;
 * char* label_name = "label";
 * tiledb_subarray_add_label_range(
 *     ctx, subarray, label_name, &start, &end, nullptr);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param subarray The subarray to add the range to.
 * @param label_name The name of the dimension label to add the range to.
 * @param start The range start.
 * @param end The range end.
 * @param stride The range stride.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 *
 * @note The stride is currently unsupported. Use 0/NULL/nullptr as the
 *     stride argument.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_add_label_range(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* label_name,
    const void* start,
    const void* end,
    const void* stride) TILEDB_NOEXCEPT;

/**
 * Adds a 1D variable-sized range for a dimension label along a subarray, which
 * is in the form (start, end). Applicable only to variable-sized dimension
 * labels.
 *
 * **Example:**
 *
 * @code{.c}
 * char start[] = "a";
 * char end[] = "bb";
 * char* label_name = "id";
 * tiledb_subarray_add_label_range_var(
 *     ctx, subarray, label_name, start, 1, end, 2);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param subarray The subarray to add the range to.
 * @param label_name The name of the dimension label to add the range to.
 * @param start The range start.
 * @param start_size The size of the range start in bytes.
 * @param end The range end.
 * @param end_size The size of the range end in bytes.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_add_label_range_var(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* label_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) TILEDB_NOEXCEPT;

/**
 * Gets the name of the dimension label for label ranges set on this dimension
 * of the subarray.
 *
 * **Example:**
 *
 * @code{.c}
 * char* label_name;
 * tiledb_subarray_get_label_name(
 *     ctx, subarray, 0, &label_name);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @pram subarray The subarray.
 * @param dim_idx The dimension index the label ranges are set on.
 * @param label_name The output name of the dimension label.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_get_label_name(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const char** label_name) TILEDB_NOEXCEPT;

/**
 * Retrieves a specific label range of the subarray from the ranges set for the
 * given dimension label name.
 *
 * **Example:**
 *
 * @code{.c}
 * const void* start;
 * const void* end;
 * const void* stride;
 * tiledb_subarray_get_label_range(
 *     ctx, subarray, label_name, range_idx, &start, &end, &stride);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param subarray The subarray.
 * @param label_name The name of the dimension label to retrieve the range from.
 * @param range_idx The index of the range to retrieve.
 * @param start Receives the retrieved range start.
 * @param end Receives the retrieved range end.
 * @param stride Receives the retrieved range stride.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_get_label_range(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* label_name,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) TILEDB_NOEXCEPT;

/**
 * Retrieves the number of label ranges set for the subarray for the
 * dimension label with the given name.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t range_num;
 * tiledb_subarray_get_label_range_num(
 *     ctx, subarray, label_name, &range_num);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param subarray The subarray.
 * @param label_name The name of the dimension label whose range number to
 *     retrieve.
 * @param range_num Receives the retrieved number of ranges.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_get_label_range_num(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* label_name,
    uint64_t* range_num) TILEDB_NOEXCEPT;

/**
 * Retrieves a specific range of the subarray for a
 * variable-length dimension label at the given name
 *
 * **Example:**
 *
 * @code{.c}
 * const void* start;
 * const void* end;
 * tiledb_subarray_get_label_range_var(
 *     ctx, subarray, label_name, range_idx, &start, &end);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param subarray The subarray.
 * @param label_name The name of the dimension to retrieve the range from.
 * @param range_idx The index of the range to retrieve.
 * @param start Receives the retrieved range start.
 * @param end Receives the retrieved range end.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_get_label_range_var(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* label_name,
    uint64_t range_idx,
    void* start,
    void* end) TILEDB_NOEXCEPT;

/**
 * Retrieves a range's start and end size for a given variable-length
 * dimension label with the given dimension label name and at the given range
 * index.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t start_size;
 * uint64_t end_size;
 * tiledb_subarray_get_label_range_var_size(
 *     ctx, subarray, label_name, range_idx, &start_size, &end_size);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param subarray The subarray.
 * @param label_name The name of the dimension label to retrieve the range from.
 * @param range_idx The index of the range to retrieve.
 * @param start_size Receives the retrieved range start size in bytes
 * @param end_size Receives the retrieved range end size in bytes
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_get_label_range_var_size(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* label_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) TILEDB_NOEXCEPT;

/**
 * Checks whether the subarray has label ranges set on the requested dimension.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t has_label_range;
 * tiledb_array_schema_has_label_ranges(
 *     ctx, array_schema, "label_0", &has_label_range);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param subarray The subarray.
 * @param dim_idx The index of the dimension to check for label ranges.
 * @param has_label_range Set to `1` if the subarray has label ranges set
 *      on the given dimension, else `0`.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_has_label_ranges(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const uint32_t dim_idx,
    int32_t* has_label_range) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_DIMENSION_LABEL_EXPERIMENTAL_H
