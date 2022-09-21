/**
 * @file tiledb/sm/c_api/experimental/tiledb_dimension_label.h
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

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
#define TILEDB_LABEL_ORDER_ENUM(id) TILEDB_##id
#include "tiledb_enum_experimental.h"
#undef TILEDB_LABEL_ORDER_ENUM
} tiledb_label_order_t;

typedef struct tiledb_dimension_label_schema_t tiledb_dimension_label_schema_t;

/**
 * Adds a dimension label to a array schema.
 *
 * @code{.c}
 * int64_t dim_domain[] = {1, 10};
 * int64_t tile_extent = 5;
 * double label_domain [] = {-10.0, 10.0};
 * double label_tile_extent = 4.0;
 * tiledb_dimension_label_schema_t* dim_label;
 * tiledb_dimension_label_schema_alloc(
 *     ctx,
 *     TILEDB_INCREASING_LABELS,
 *     TILEDB_INT64,
 *     dim_domain,
 *     &tile_extent,
 *     TILEDB_FLOAT64,
 *     label_domain,
 *     &label_tile_extent,
 *     &dim_label);
 * tiledb_array_schema_t* label_array_schema;
 * tiledb_array_schema_add_dimension_label(
 *     ctx,
 *     array_schema,
 *     0,
 *     dim_label_schema);
 * @endcode
 *
 */
TILEDB_EXPORT int32_t tiledb_array_schema_add_dimension_label(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const uint32_t dim_id,
    const char* name,
    tiledb_dimension_label_schema_t* dim_label_schema) TILEDB_NOEXCEPT;

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
TILEDB_EXPORT int32_t tiledb_array_schema_has_dimension_label(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char* name,
    int32_t* has_dim_label) TILEDB_NOEXCEPT;

/**
 * Creates a TileDB dimension label object.
 *
 * **Example:**
 *
 * @code{.c}
 * int64_t dim_domain[] = {1, 10};
 * int64_t tile_extent = 5;
 * double label_domain [] = {-10.0, 10.0};
 * double label_tile_extent = 4.0;
 * tiledb_dimension_label_schema_t* dim_label;
 * tiledb_dimension_label_schema_alloc(
 *     ctx,
 *     TILEDB_INCREASING_LABELS,
 *     TILEDB_INT64,
 *     dim_domain,
 *     &tile_extent,
 *     TILEDB_FLOAT64,
 *     label_domain,
 *     &label_tile_extent,
 *     &dim_label);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param label_type The label type.
 * @param index_type The datatype for the original dimension data. Must be the
 *     same as the dimension the dimension label is applied to.
 * @param index_domain The range the original dimension is defined on. Must be
 *     the same as the dimension the dimension label is applied to.
 * @param index_tile_extent The tile extent for the original dimension data on
 *     the dimension label.
 * @param label_type The datatype for the new label dimension data.
 * @param label_dim_domain The range the label data is defined on domain.
 * @param label_tile_extent The tile extent for the label data.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_dimension_label_schema_alloc(
    tiledb_ctx_t* ctx,
    tiledb_label_order_t label_order,
    tiledb_datatype_t index_type,
    const void* index_domain,
    const void* index_tile_extent,
    tiledb_datatype_t label_type,
    const void* label_domain,
    const void* label_tile_extent,
    tiledb_dimension_label_schema_t** dim_label_schema) TILEDB_NOEXCEPT;

/**
 * Destroys a TileDB dimension label schema, freeing associated memory.
 *
 * @code{.c}
 * tiledb_dimension_t* dim;
 * int64_t dim_domain[] = {1, 10};
 * int64_t tile_extent = 5;
 * tiledb_dimension_alloc(
 *     ctx, "dim_0", TILEDB_INT64, dim_domain, &tile_extent, &dim);
 * double label_domain [] = {-10.0, 10.0};
 * double label_tile_extent = 4.0;
 * tiledb_dimension_label_schema_t* dim_label_schema;
 * tiledb_dimension_label_schema_alloc(
 *     ctx,
 *     TILEDB_INCREASING_LABELS,
 *     dim,
 *     TILEDB_FLOAT64,
 *     label_domain,
 *     &label_tile_extent,
 *     &dim_label_schema);
 * tiledb_dimension_label_schema_free(&dim_label_schema)
 * @endcode
 *
 * @param dim_label_schema The dimension label schema to be destroyed.
 */
TILEDB_EXPORT void tiledb_dimension_label_schema_free(
    tiledb_dimension_label_schema_t** dim_label_schema) TILEDB_NOEXCEPT;

/**
 * Sets the buffer for an dimension label to a query, which will
 * either hold the values to be written (if it is a write query), or will hold
 * the results from a read query.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t label1[100];
 * uint64_t label1_size = sizeof(label1);
 * tiledb_query_set_label_data_buffer(
 *     ctx, query, "label1", label1, &label1_size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param name The name of the dimension label to set the buffer for.
 * @param buffer The buffer that either have the input data to be written,
 *     or will hold the data to be read.
 * @param buffer_size In the case of writes, this is the size of `buffer`
 *     in bytes. In the case of reads, this initially contains the allocated
 *     size of `buffer`, but after the termination of the query
 *     it will contain the size of the useful (read) data in `buffer`.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_set_label_data_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    void* buffer,
    uint64_t* buffer_size) TILEDB_NOEXCEPT;

/**
 * Sets the starting offsets of each cell value in the data buffer.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t label1[100];
 * uint64_t label1_size = sizeof(label1);
 * tiledb_query_set_label_offsets_buffer(
 *     ctx, query, "label1", label1, &label1_size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param name The name of the dimension label to set the buffer for.
 * @param buffer This buffer holds the starting offsets of each cell value in
 *     the buffer set by `tiledb_query_set_label_data_buffer`.
 * @param buffer_size In the case of writes, it is the size of `buffer_off`
 *     in bytes. In the case of reads, this initially contains the allocated
 *     size of `buffer_off`, but after the *end of the query*
 *     (`tiledb_query_submit`) it will contain the size of the useful (read)
 *     data in `buffer_off`.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_set_label_offsets_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint64_t* buffer,
    uint64_t* buffer_size) TILEDB_NOEXCEPT;

/**
 * Gets the buffer of a fixed-sized dimension label from a query. If the
 * buffer has not been set, then `buffer` is set to `nullptr`.
 *
 * **Example:**
 *
 * @code{.c}
 * int* label1;
 * uint64_t* label1_size;
 * tiledb_query_get_label_data_buffer(
 *     ctx, query, "label1", &label1, &label1_size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param name The name of the dimension label to get the buffer for.
 * @param buffer The buffer to retrieve.
 * @param buffer_size A pointer to the size of the buffer. Note that this is
 *     a double pointer and returns the original variable address from
 *     `tiledb_query_set_label_data_buffer`.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_label_data_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    void** buffer,
    uint64_t** buffer_size) TILEDB_NOEXCEPT;

/**
 * Gets the starting offsets of each cell value in the data buffer.
 *
 * **Example:**
 *
 * @code{.c}
 * int* label1;
 * uint64_t* label1_size;
 * tiledb_query_get_label_offsets_buffer(
 *     ctx, query, "label1", &label1, &label1_size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param name The name of the dimension label to get the buffer for.
 * @param buffer The buffer to retrieve.
 * @param buffer_size A pointer to the size of the buffer. Note that this is
 *     a double pointer and returns the original variable address from
 *     `tiledb_query_set_label_offsets_buffer`.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_label_offsets_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint64_t** buffer,
    uint64_t** buffer_size) TILEDB_NOEXCEPT;

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
 * @param query The subarray to add the range to.
 * @param label_name The name of the dimension label to add the range to.
 * @param start The range start.
 * @param end The range end.
 * @param stride The range stride.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 *
 * @note The stride is currently unsupported. Use 0/NULL/nullptr as the
 *     stride argument.
 */
TILEDB_EXPORT int32_t tiledb_subarray_add_label_range(
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
TILEDB_EXPORT int32_t tiledb_subarray_add_label_range_var(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* label_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) TILEDB_NOEXCEPT;

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
 *     ctx, query, label_name, range_idx, &start, &end, &stride);
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
TILEDB_EXPORT int32_t tiledb_subarray_get_label_range(
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
 * retrieve.
 * @param range_num Receives the retrieved number of ranges.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_subarray_get_label_range_num(
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
TILEDB_EXPORT int32_t tiledb_subarray_get_label_range_var(
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
TILEDB_EXPORT int32_t tiledb_subarray_get_label_range_var_size(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* label_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_DIMENSION_LABEL_EXPERIMENTAL_H
