/**
 * @file tiledb/api/c_api/query/query_api_external.h
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
 * This file declares the query C API for TileDB.
 */

#ifndef TILEDB_CAPI_QUERY_API_EXTERNAL_H
#define TILEDB_CAPI_QUERY_API_EXTERNAL_H

#include "../api_external_common.h"
#include "../array/array_api_external.h"
#include "../array_schema/array_schema_api_external.h"
#include "../config/config_api_external.h"
#include "../context/context_api_external.h"
#include "../query_condition/query_condition_api_external.h"
#include "../subarray/subarray_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/** A TileDB query object. */
typedef struct tiledb_query_handle_t tiledb_query_t;

/** Query status. */
typedef enum {
/** Helper macro for defining query status enums. */
#define TILEDB_QUERY_STATUS_ENUM(id) TILEDB_##id
#include "tiledb/sm/c_api/tiledb_enum.h"
#undef TILEDB_QUERY_STATUS_ENUM
} tiledb_query_status_t;

/**
 * Returns a string representation of the given query type.
 *
 * @param query_type Query type
 * @param str Set to point to a constant string representation of the query type
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_query_type_to_str(
    tiledb_query_type_t query_type, const char** str) TILEDB_NOEXCEPT;

/**
 * Parses a query type from the given string.
 *
 * @param str String representation to parse
 * @param query_type Set to the parsed query type
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_query_type_from_str(
    const char* str, tiledb_query_type_t* query_type) TILEDB_NOEXCEPT;

/**
 * Creates a TileDB query object. Note that the query object is associated
 * with a specific array object. The query type (read or write) is inferred
 * from the array object, which was opened with a specific query type.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "file:///my_array", &array);
 * tiledb_array_open(ctx, array, TILEDB_WRITE);
 * tiledb_query_t* query;
 * tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query object to be created.
 * @param array An opened array object.
 * @param query_type The query type. This must comply with the query type
 *     `array` was opened.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_alloc(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_query_type_t query_type,
    tiledb_query_t** query) TILEDB_NOEXCEPT;

/**
 * Retrieves the stats from a Query.
 *
 * **Example:**
 *
 * @code{.c}
 * char* stats_json;
 * tiledb_query_get_stats(ctx, query, &stats_json);
 * // Use the string
 * tiledb_stats_free_str(&stats_json);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query object.
 * @param stats_json The output json. The caller takes ownership
 *   of the c-string and must free it using tiledb_stats_free_str().
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_stats(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    char** stats_json) TILEDB_NOEXCEPT;

/**
 * Set the query config
 *
 * Setting the query config will also set the subarray configuration in order to
 * maintain existing behavior. If you wish the subarray to have a different
 * configuration than the query, set it after calling tiledb_query_set_config.
 *
 * Setting the configuration with this function overrides the following
 * Query-level parameters only:
 *
 * - `sm.memory_budget`
 * - `sm.memory_budget_var`
 * - `sm.var_offsets.mode`
 * - `sm.var_offsets.extra_element`
 * - `sm.var_offsets.bitsize`
 * - `sm.check_coord_dups`
 * - `sm.check_coord_oob`
 * - `sm.check_global_order`
 * - `sm.dedup_coords`
 */
TILEDB_EXPORT int32_t tiledb_query_set_config(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_config_t* config) TILEDB_NOEXCEPT;

/**
 * Retrieves the config from a Query.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_config_t* config;
 * tiledb_query_get_config(ctx, vfs, &config);
 * // Make sure to free the retrieved config
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query object.
 * @param config The config to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_config(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_config_t** config) TILEDB_NOEXCEPT;

/**
 * Indicates that the query will write or read a subarray, and provides
 * the appropriate information.
 *
 * **Example:**
 *
 * The following sets a 2D subarray [0,10], [20, 30] to the query.
 *
 * @code{.c}
 * tiledb_subarray_t *subarray;
 * tiledb_subarray_alloc(ctx, array, &subarray);
 * uint64_t subarray_v[] = { 0, 10, 20, 30};
 * tiledb_subarray_set_subarray(ctx, subarray, subarray_v);
 * tiledb_query_set_subarray_t(ctx, query, subarray);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param subarray The subarray by which the array read/write will be
 *     constrained. For the case of writes, this is meaningful only
 *     for dense arrays.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 *
 * @note This will error if the query is already initialized.
 *
 * @note This will error for writes to sparse arrays.
 */
TILEDB_EXPORT int32_t tiledb_query_set_subarray_t(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const tiledb_subarray_t* subarray) TILEDB_NOEXCEPT;

/**
 * Sets the buffer for an attribute/dimension to a query, which will
 * either hold the values to be written (if it is a write query), or will hold
 * the results from a read query.
 *
 * The caller owns the `buffer` provided and is responsible for freeing the
 * memory associated with it. For writes, the buffer holds values to be written
 * which can be freed at any time after query completion. For reads, the buffer
 * is allocated by the caller and will contain data read by the query after
 * completion. The freeing of this memory is up to the caller once they are done
 * referencing the read data.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t a1[100];
 * uint64_t a1_size = sizeof(a1);
 * tiledb_query_set_data_buffer(ctx, query, "a1", a1, &a1_size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param name The attribute/dimension to set the buffer for. Note that
 *     zipped coordinates have special name `TILEDB_COORDS`.
 * @param buffer The buffer that either have the input data to be written,
 *     or will hold the data to be read.
 * @param buffer_size In the case of writes, this is the size of `buffer`
 *     in bytes. In the case of reads, this initially contains the allocated
 *     size of `buffer`, but after the termination of the query
 *     it will contain the size of the useful (read) data in `buffer`.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_set_data_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    void* buffer,
    uint64_t* buffer_size) TILEDB_NOEXCEPT;

/**
 * Sets the starting offsets of each cell value in the data buffer.
 *
 * The caller owns the `buffer` provided and is responsible for freeing the
 * memory associated with it. For writes, the buffer holds offsets to be written
 * which can be freed at any time after query completion. For reads, the buffer
 * is allocated by the caller and will contain offset data read by the query
 * after completion. The freeing of this memory is up to the caller once they
 * are done referencing the read data.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t a1[100];
 * uint64_t a1_size = sizeof(a1);
 * tiledb_query_set_offsets_buffer(ctx, query, "a1", a1, &a1_size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param name The attribute/dimension to set the buffer for. Note that
 *     zipped coordinates have special name `TILEDB_COORDS`.
 * @param buffer This buffer holds the starting offsets
 *     of each cell value in `buffer_val`.
 * @param buffer_size In the case of writes, it is the size of `buffer_off`
 *     in bytes. In the case of reads, this initially contains the allocated
 *     size of `buffer_off`, but after the *end of the query*
 *     (`tiledb_query_submit`) it will contain the size of the useful (read)
 *     data in `buffer_off`.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_set_offsets_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint64_t* buffer,
    uint64_t* buffer_size) TILEDB_NOEXCEPT;

/**
 * Sets the validity byte map that has exactly one value for each value in the
 * data buffer.
 *
 * The caller owns the `buffer` provided and is responsible for freeing the
 * memory associated with it. For writes, the buffer holds validity values to be
 * written which can be freed at any time after query completion. For reads, the
 * buffer is allocated by the caller and will contain the validity map read by
 * the query after completion. The freeing of this memory is up to the caller
 * once they are done referencing the read data.
 *
 * **Example:**
 *
 * @code{.c}
 * uint8_t a1[100];
 * uint64_t a1_size = sizeof(a1);
 * tiledb_query_set_validity_buffer(ctx, query, "a1", a1, &a1_size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param name The attribute/dimension to set the buffer for. Note that
 *     zipped coordinates have special name `TILEDB_COORDS`.
 * @param buffer The validity byte map that has exactly
 *     one value for each value in `buffer`.
 * @param buffer_size In the case of writes, this is the
 *     size of `buffer_validity_bytemap` in bytes. In the case of reads,
 *     this initially contains the allocated size of `buffer_validity_bytemap`,
 *     but after the termination of the query it will contain the size of the
 *     useful (read) data in `buffer_validity_bytemap`.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_set_validity_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint8_t* buffer,
    uint64_t* buffer_size) TILEDB_NOEXCEPT;

/**
 * Gets the buffer of a fixed-sized attribute/dimension from a query. If the
 * buffer has not been set, then `buffer` is set to `nullptr`.
 *
 * **Example:**
 *
 * @code{.c}
 * int* a1;
 * uint64_t* a1_size;
 * tiledb_query_get_data_buffer(ctx, query, "a1", &a1, &a1_size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param name The attribute/dimension to get the buffer for. Note that the
 *     zipped coordinates have special name `TILEDB_COORDS`.
 * @param buffer The buffer to retrieve.
 * @param buffer_size A pointer to the size of the buffer. Note that this is
 *     a double pointer and returns the original variable address from
 *     `set_buffer`.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_data_buffer(
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
 * int* a1;
 * uint64_t* a1_size;
 * tiledb_query_get_offsets_buffer(ctx, query, "a1", &a1, &a1_size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param name The attribute/dimension to get the buffer for. Note that the
 *     zipped coordinates have special name `TILEDB_COORDS`.
 * @param buffer The buffer to retrieve.
 * @param buffer_size A pointer to the size of the buffer. Note that this is
 *     a double pointer and returns the original variable address from
 *     `set_buffer`.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_offsets_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint64_t** buffer,
    uint64_t** buffer_size) TILEDB_NOEXCEPT;

/**
 * Gets the validity byte map that has exactly one value for each value in the
 * data buffer.
 *
 * **Example:**
 *
 * @code{.c}
 * int* a1;
 * uint64_t* a1_size;
 * tiledb_query_get_validity_buffer(ctx, query, "a1", &a1, &a1_size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param name The attribute/dimension to get the buffer for. Note that the
 *     zipped coordinates have special name `TILEDB_COORDS`.
 * @param buffer The buffer to retrieve.
 * @param buffer_size A pointer to the size of the buffer. Note that this is
 *     a double pointer and returns the original variable address from
 *     `set_buffer`.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_validity_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint8_t** buffer,
    uint64_t** buffer_size) TILEDB_NOEXCEPT;

/**
 * Sets the layout of the cells to be written or read.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param layout For a write query, this specifies the order of the cells
 *     provided by the user in the buffers. For a read query, this specifies
 *     the order of the cells that will be retrieved as results and stored
 *     in the user buffers. The layout can be one of the following:
 *    - `TILEDB_COL_MAJOR`:
 *      This means column-major order with respect to the subarray.
 *    - `TILEDB_ROW_MAJOR`:
 *      This means row-major order with respect to the subarray.
 *    - `TILEDB_GLOBAL_ORDER`:
 *      This means that cells are stored or retrieved in the array global
 *      cell order.
 *    - `TILEDB_UNORDERED`:
 *      This is applicable only to reads and writes for sparse arrays, or for
 *      sparse writes to dense arrays. For writes, it specifies that the cells
 *      are unordered and, hence, TileDB must sort the cells in the global cell
 *      order prior to writing. For reads, TileDB will return the cells without
 *      any particular order, which will often lead to better performance.
 * * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_set_layout(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_layout_t layout) TILEDB_NOEXCEPT;

/**
 * Sets the query condition to be applied on a read.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_condition_t* query_condition;
 * tiledb_query_condition_alloc(ctx, &query_condition);
 * uint32_t value = 5;
 * tiledb_query_condition_init(
 *   ctx, query_condition, "longitude", &value, sizeof(value), TILEDB_LT);
 * tiledb_query_set_condition(ctx, query, query_condition);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param cond The TileDB query condition.
 */
TILEDB_EXPORT int32_t tiledb_query_set_condition(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const tiledb_query_condition_t* cond) TILEDB_NOEXCEPT;

/**
 * Flushes all internal state of a query object and finalizes the query.
 * This is applicable only to global layout writes. It has no effect for
 * any other query type.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_t* query;
 * // ... Your code here ... //
 * tiledb_query_finalize(ctx, query);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query object to be flushed.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t
tiledb_query_finalize(tiledb_ctx_t* ctx, tiledb_query_t* query) TILEDB_NOEXCEPT;

/**
 * Submits and finalizes the query.
 * This is applicable only to global layout writes. The function will
 * error out if called on a query with non global layout.
 * Its purpose is to submit the final chunk (partial or full tile) in
 * a global order write query.
 * `tiledb_query_submit_and_finalize` drops the tile alignment restriction
 * of the buffers (i.e. compared to the regular global layout submit call)
 * given the last chunk of a global order write is most frequently smaller
 * in size than a tile.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_t* query;
 * while (stop_condition) {
 *   tiledb_query_set_buffer(ctx, query, attr, tile_aligned_buffer, &size);
 *   tiledb_query_submit(ctx, query);
 * }
 * tiledb_query_set_buffer(ctx, query, attr, final_chunk, &size);
 * tiledb_query_submit_and_finalize(ctx, query);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query object to be flushed.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_submit_and_finalize(
    tiledb_ctx_t* ctx, tiledb_query_t* query) TILEDB_NOEXCEPT;

/**
 * Frees a TileDB query object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_free(&query);
 * @endcode
 *
 * @param query The query object to be deleted.
 */
TILEDB_EXPORT void tiledb_query_free(tiledb_query_t** query) TILEDB_NOEXCEPT;

/**
 * Submits a TileDB query.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_submit(ctx, query);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query to be submitted.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note `tiledb_query_finalize` must be invoked after finish writing in
 *     global layout (via repeated invocations of `tiledb_query_submit`),
 *     in order to flush any internal state.
 *
 * @note For the case of reads, if the returned status is `TILEDB_INCOMPLETE`,
 *    TileDB could not fit the entire result in the user's buffers. In this
 *    case, the user should consume the read results (if any), optionally
 *    reset the buffers with `tiledb_query_set_buffer`, and then resubmit the
 *    query until the status becomes `TILEDB_COMPLETED`. If all buffer sizes
 *    after the termination of this function become 0, then this means that
 *    **no** useful data was read into the buffers, implying that larger
 *    buffers are needed for the query to proceed. In this case, the users
 *    must reallocate their buffers (increasing their size), reset the buffers
 *    with `tiledb_query_set_buffer`, and resubmit the query.
 */
TILEDB_EXPORT int32_t
tiledb_query_submit(tiledb_ctx_t* ctx, tiledb_query_t* query) TILEDB_NOEXCEPT;

/**
 * Checks if the query has returned any results. Applicable only to
 * read queries; it sets `has_results` to `0 in the case of writes.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t has_results;
 * tiledb_query_has_results(ctx, query, &has_results);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query.
 * @param has_results Set to `1` if the query returned results and `0`
 *     otherwise.
 * @return `TILEDB_OK` upon success, and `TILEDB_ERR` upon error.
 */
TILEDB_EXPORT int32_t tiledb_query_has_results(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    int32_t* has_results) TILEDB_NOEXCEPT;

/**
 * Retrieves the status of a query.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_status_t status;
 * tiledb_query_get_status(ctx, query, &status);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query.
 * @param status The query status to be retrieved.
 * @return `TILEDB_OK` upon success, and `TILEDB_ERR` upon error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_status(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_query_status_t* status) TILEDB_NOEXCEPT;

/**
 * Retrieves the query type.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_type_t query_type;
 * tiledb_query_get_status(ctx, query, &query_type);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query.
 * @param query_type The query type to be retrieved.
 * @return `TILEDB_OK` upon success, and `TILEDB_ERR` upon error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_type(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_query_type_t* query_type) TILEDB_NOEXCEPT;

/**
 * Retrieves the query layout.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_layout_t query_layout;
 * tiledb_query_get_layout(ctx, query, &query_layout);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query.
 * @param query_layout The query layout to be retrieved.
 * @return `TILEDB_OK` upon success, and `TILEDB_ERR` upon error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_layout(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_layout_t* query_layout) TILEDB_NOEXCEPT;

/**
 * Retrieves the query array.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_t* array;
 * tiledb_query_get_array(ctx, query, &array);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query.
 * @param array The query array to be retrieved.
 * @return `TILEDB_OK` upon success, and `TILEDB_ERR` upon error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_array(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_array_t** array) TILEDB_NOEXCEPT;

/**
 * Retrieves the estimated result size for a fixed-sized attribute/dimension.
 * This is an estimate and may not be sufficient to read all results for the
 * requested range, in particular for sparse arrays or array with
 * var-length attributes.
 * Query status must be checked and resubmitted if not complete.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t size;
 * tiledb_query_get_est_result_size(ctx, query, "a", &size);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query The query.
 * @param name The attribute/dimension name.
 * @param size The size (in bytes) to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_est_result_size(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* name,
    uint64_t* size) TILEDB_NOEXCEPT;

/**
 * Retrieves the estimated result size for a var-sized attribute/dimension.
 * This is an estimate and may not be sufficient to read all results for the
 * requested range, for sparse arrays or any array with
 * var-length attributes.
 * Query status must be checked and resubmitted if not complete.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t size_off, size_val;
 * tiledb_query_get_est_result_size_var(
 *     ctx, query, "a", &size_off, &size_val);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query The query.
 * @param name The attribute/dimension name.
 * @param size_off The size of the offsets (in bytes) to be retrieved.
 * @param size_val The size of the values (in bytes) to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_est_result_size_var(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* name,
    uint64_t* size_off,
    uint64_t* size_val) TILEDB_NOEXCEPT;

/**
 * Retrieves the estimated result size for a fixed-sized, nullable attribute.
 * This is an estimate and may not be sufficient to read all results for the
 * requested range, for sparse arrays or any array with
 * var-length attributes.
 * Query status must be checked and resubmitted if not complete.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t size_val;
 * uint64_t size_validity;
 * tiledb_query_get_est_result_size_nullable(ctx, query, "a", &size_val,
 * &size_validity);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query The query.
 * @param name The attribute name.
 * @param size_val The size of the values (in bytes) to be retrieved.
 * @param size_validity The size of the validity values (in bytes) to be
 * retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_est_result_size_nullable(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* name,
    uint64_t* size_val,
    uint64_t* size_validity) TILEDB_NOEXCEPT;

/**
 * Retrieves the estimated result size for a var-sized, nullable attribute.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t size_off, size_val, size_validity;
 * tiledb_query_get_est_result_size_var_nullable(
 *     ctx, query, "a", &size_off, &size_val, &size_validity);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query The query.
 * @param name The attribute name.
 * @param size_off The size of the offsets (in bytes) to be retrieved.
 * @param size_val The size of the values (in bytes) to be retrieved.
 * @param size_validity The size of the validity values (in bytes) to be
 * retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_est_result_size_var_nullable(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* name,
    uint64_t* size_off,
    uint64_t* size_val,
    uint64_t* size_validity) TILEDB_NOEXCEPT;

/**
 * Retrieves the number of written fragments. Applicable only to WRITE
 * queries.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t num;
 * tiledb_query_get_fragment_num(ctx, query, &num);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query The query.
 * @param num The number of written fragments to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_fragment_num(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint32_t* num) TILEDB_NOEXCEPT;

/**
 * Retrieves the URI of the written fragment with the input index. Applicable
 * only to WRITE queries.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* uri;
 * tiledb_query_get_fragment_uri(
 *     ctx, query, 0, &uri);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query The query.
 * @param idx The index of the written fragment.
 * @param uri The URI of the written fragment to be returned.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note Make sure to make a copy of `uri` after its retrieval, as the
 *     constant pointer may be updated internally as new fragments
 *     are being written.
 */
TILEDB_EXPORT int32_t tiledb_query_get_fragment_uri(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint64_t idx,
    const char** uri) TILEDB_NOEXCEPT;

/**
 * Retrieves the timestamp range of the written fragment with the input index.
 * Applicable only to WRITE queries.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t t1, t2;
 * tiledb_query_get_fragment_timestamp_range(
 *     ctx, query, 0, &t1, &t2);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query The query.
 * @param idx The index of the written fragment.
 * @param t1 The start value of the timestamp range of the
 *     written fragment to be returned.
 * @param t2 The end value of the timestamp range of the
 *     written fragment to be returned.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_fragment_timestamp_range(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint64_t idx,
    uint64_t* t1,
    uint64_t* t2) TILEDB_NOEXCEPT;

/**
 * Return a TileDB subarray object from the given query.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_subarray_t* subarray;
 * tiledb_query_get_subarray_t(array, &subarray);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query An open Query object.
 * @param subarray The retrieved subarray object if available.
 * @return `TILEDB_OK` for success or `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT
int32_t tiledb_query_get_subarray_t(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    tiledb_subarray_t** subarray) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_QUERY_API_EXTERNAL_H
