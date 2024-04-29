/*
 * @file   tiledb_deprecated.h
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
 * This file declares the deprecated C API for TileDB.
 */

#ifndef TILEDB_DEPRECATED_H
#define TILEDB_DEPRECATED_H

#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Retrieves the schema of an encrypted array from the disk, creating an array
 * schema struct.
 *
 * **Example:**
 *
 * @code{.c}
 * // Load AES-256 key from disk, environment variable, etc.
 * uint8_t key[32] = ...;
 * tiledb_array_schema_t* array_schema;
 * tiledb_array_schema_load_with_key(
 *     ctx, "s3://tiledb_bucket/my_array", TILEDB_AES_256_GCM,
 *     key, sizeof(key), &array_schema);
 * // Make sure to free the array schema in the end
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_uri The array whose schema will be retrieved.
 * @param encryption_type The encryption type to use.
 * @param encryption_key The encryption key to use.
 * @param key_length Length in bytes of the encryption key.
 * @param array_schema The array schema to be retrieved, or `NULL` upon error.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_schema_load_with_key(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    tiledb_array_schema_t** array_schema) TILEDB_NOEXCEPT;

/**
 * Indicates that the query will write or read a subarray, and provides
 * the appropriate information.
 *
 * **Example:**
 *
 * The following sets a 2D subarray [0,10], [20, 30] to the query.
 *
 * @code{.c}
 * uint64_t subarray[] = { 0, 10, 20, 30};
 * tiledb_query_set_subarray(ctx, query, subarray);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param subarray The subarray in which the array read/write will be
 *     constrained on. It should be a sequence of [low, high] pairs (one
 *     pair per dimension). For the case of writes, this is meaningful only
 *     for dense arrays. Note that `subarray` must have the same type as the
 *     domain.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 *
 * @note This will error if the query is already initialized.
 *
 * @note This function will error for writes to sparse arrays.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_set_subarray(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const void* subarray) TILEDB_NOEXCEPT;

/**
 * Submits a TileDB query in asynchronous mode.
 *
 * **Examples:**
 *
 * Submit without a callback.
 *
 * @code{.c}
 * tiledb_query_submit_async(ctx, query, NULL, NULL);
 * @endcode
 *
 * Submit with a callback function `print` that takes as input message
 * `msg` and prints it upon completion of the query.
 *
 * @code{.c}
 * const char* msg = "Query completed";
 * tiledb_query_submit_async(ctx, &query, foo, msg);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query to be submitted.
 * @param callback The function to be called when the query completes.
 * @param callback_data The data to be passed to the \p callback function.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
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
 *
 * @note \p callback will be executed in a thread managed by TileDB's internal
 *    thread pool. To allow TileDB to reuse the thread and avoid starving the
 *    thread pool, long-running callbacks should be dispatched to another
 *    thread.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_submit_async(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    void (*callback)(void*),
    void* callback_data) TILEDB_NOEXCEPT;

/**
 * Adds a 1D range along a subarray dimension index, which is in the form
 * (start, end, stride). The datatype of the range components
 * must be the same as the type of the domain of the array in the query.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t dim_idx = 2;
 * int64_t start = 10;
 * int64_t end = 20;
 * tiledb_query_add_range(ctx, query, dim_idx, &start, &end, nullptr);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query to add the range to.
 * @param dim_idx The index of the dimension to add the range to.
 * @param start The range start.
 * @param end The range end.
 * @param stride The range stride.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note The stride is currently unsupported. Use `nullptr` as the
 *     stride argument.
 */

TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_add_range(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    uint32_t dim_idx,
    const void* start,
    const void* end,
    const void* stride) TILEDB_NOEXCEPT;

/**
 * Adds a 1D range along a subarray dimension name, which is in the form
 * (start, end, stride). The datatype of the range components
 * must be the same as the type of the domain of the array in the query.
 *
 * **Example:**
 *
 * @code{.c}
 * char* dim_name = "rows";
 * int64_t start = 10;
 * int64_t end = 20;
 * tiledb_query_add_range_by_name(ctx, query, dim_name, &start, &end, nullptr);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query to add the range to.
 * @param dim_name The name of the dimension to add the range to.
 * @param start The range start.
 * @param end The range end.
 * @param stride The range stride.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note The stride is currently unsupported. Use `nullptr` as the
 *     stride argument.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_add_range_by_name(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
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
 * tiledb_query_add_range_var(ctx, query, dim_idx, start, 1, end, 2);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query to add the range to.
 * @param dim_idx The index of the dimension to add the range to.
 * @param start The range start.
 * @param start_size The size of the range start in bytes.
 * @param end The range end.
 * @param end_size The size of the range end in bytes.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_add_range_var(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
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
 * tiledb_query_add_range_var_by_name(ctx, query, dim_name, start, 1, end, 2);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query to add the range to.
 * @param dim_name The name of the dimension to add the range to.
 * @param start The range start.
 * @param start_size The size of the range start in bytes.
 * @param end The range end.
 * @param end_size The size of the range end in bytes.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_add_range_var_by_name(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
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
 * tiledb_query_get_range_num(ctx, query, dim_idx, &range_num);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query The query.
 * @param dim_idx The index of the dimension whose range number to retrieve.
 * @param range_num The number of ranges to retrieve.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_range_num(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint32_t dim_idx,
    uint64_t* range_num) TILEDB_NOEXCEPT;

/**
 * Retrieves the number of ranges of the query subarray along a given dimension
 * name.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t range_num;
 * tiledb_query_get_range_num_from_name(ctx, query, dim_name, &range_num);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query The query.
 * @param dim_name The name of the dimension whose range number to retrieve.
 * @param range_num The number of ranges to retrieve.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_range_num_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* dim_name,
    uint64_t* range_num) TILEDB_NOEXCEPT;

/**
 * Retrieves a specific range of the query subarray along a given dimension
 * index.
 *
 * **Example:**
 *
 * @code{.c}
 * const void* start;
 * const void* end;
 * const void* stride;
 * tiledb_query_get_range(
 *     ctx, query, dim_idx, range_idx, &start, &end, &stride);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query The query.
 * @param dim_idx The index of the dimension to retrieve the range from.
 * @param range_idx The index of the range to retrieve.
 * @param start The range start to retrieve.
 * @param end The range end to retrieve.
 * @param stride The range stride to retrieve.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_range(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint32_t dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) TILEDB_NOEXCEPT;

/**
 * Retrieves a specific range of the query subarray along a given dimension
 * name.
 *
 * **Example:**
 *
 * @code{.c}
 * const void* start;
 * const void* end;
 * const void* stride;
 * tiledb_query_get_range_from_name(
 *     ctx, query, dim_name, range_idx, &start, &end, &stride);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query The query.
 * @param dim_name The name of the dimension to retrieve the range from.
 * @param range_idx The index of the range to retrieve.
 * @param start The range start to retrieve.
 * @param end The range end to retrieve.
 * @param stride The range stride to retrieve.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_range_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
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
 * tiledb_query_get_range_var_size(
 *     ctx, query, dim_idx, range_idx, &start_size, &end_size);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query The query.
 * @param dim_idx The index of the dimension to retrieve the range from.
 * @param range_idx The index of the range to retrieve.
 * @param start_size range start size in bytes
 * @param end_size range end size in bytes
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_range_var_size(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
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
 * tiledb_query_get_range_var_size_from_name(
 *     ctx, query, dim_name, range_idx, &start_size, &end_size);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query The query.
 * @param dim_name The name of the dimension to retrieve the range from.
 * @param range_idx The index of the range to retrieve.
 * @param start_size range start size in bytes
 * @param end_size range end size in bytes
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_range_var_size_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) TILEDB_NOEXCEPT;

/**
 * Retrieves a specific range of the query subarray along a given
 * variable-length dimension index.
 *
 * **Example:**
 *
 * @code{.c}
 * const void* start;
 * const void* end;
 * tiledb_query_get_range_var(
 *     ctx, query, dim_idx, range_idx, &start, &end);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query The query.
 * @param dim_idx The index of the dimension to retrieve the range from.
 * @param range_idx The index of the range to retrieve.
 * @param start The range start to retrieve.
 * @param end The range end to retrieve.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_range_var(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint32_t dim_idx,
    uint64_t range_idx,
    void* start,
    void* end) TILEDB_NOEXCEPT;

/**
 * Retrieves a specific range of the query subarray along a given
 * variable-length dimension name.
 *
 * **Example:**
 *
 * @code{.c}
 * const void* start;
 * const void* end;
 * tiledb_query_get_range_var_from_name(
 *     ctx, query, dim_name, range_idx, &start, &end);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query The query.
 * @param dim_name The name of the dimension to retrieve the range from.
 * @param range_idx The index of the range to retrieve.
 * @param start The range start to retrieve.
 * @param end The range end to retrieve.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_range_var_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* dim_name,
    uint64_t range_idx,
    void* start,
    void* end) TILEDB_NOEXCEPT;

/**
 * Deletes array fragments written between the input timestamps.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_delete_fragments(
 *   ctx, array, "hdfs:///temp/my_array", 0, UINT64_MAX);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array The array to delete the fragments from.
 * @param uri The URI of the fragments' parent Array.
 * @param timestamp_start The epoch timestamp in milliseconds.
 * @param timestamp_end The epoch timestamp in milliseconds. Use UINT64_MAX for
 *   the current timestamp.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note This function was deprecated in release 2.18 in favor of
 * tiledb_array_delete_fragments_v2.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_delete_fragments(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* uri,
    uint64_t timestamp_start,
    uint64_t timestamp_end) TILEDB_NOEXCEPT;

/**
 * Creates a new encrypted TileDB array given an input schema.
 *
 * Encrypted arrays can only be created through this function.
 *
 * **Example:**
 *
 * @code{.c}
 * uint8_t key[32] = ...;
 * tiledb_array_create_with_key(
 *     ctx, "hdfs:///tiledb_arrays/my_array", array_schema,
 *     TILEDB_AES_256_GCM, key, sizeof(key));
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_uri The array name.
 * @param array_schema The array schema.
 * @param encryption_type The encryption type to use.
 * @param encryption_key The encryption key to use.
 * @param key_length Length in bytes of the encryption key.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_create_with_key(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const tiledb_array_schema_t* array_schema,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length) TILEDB_NOEXCEPT;

/**
 * Depending on the consoliation mode in the config, consolidates either the
 * fragment files, fragment metadata files, or array metadata files into a
 * single file.
 *
 * **Example:**
 *
 * @code{.c}
 * uint8_t key[32] = ...;
 * tiledb_array_consolidate_with_key(
 *     ctx, "hdfs:///tiledb_arrays/my_array",
 *     TILEDB_AES_256_GCM, key, sizeof(key), nullptr);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_uri The name of the TileDB array to be consolidated.
 * @param encryption_type The encryption type to use.
 * @param encryption_key The encryption key to use.
 * @param key_length Length in bytes of the encryption key.
 * @param config Configuration parameters for the consolidation
 *     (`nullptr` means default, which will use the config from `ctx`).
 *     The `sm.consolidation.mode` parameter determines which type of
 *     consolidation to perform.
 *
 * @return `TILEDB_OK` on success, and `TILEDB_ERR` on error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_consolidate_with_key(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    tiledb_config_t* config) TILEDB_NOEXCEPT;

/**
 * Gets the name of a fragment. Deprecated, use
 * \p tiledb_fragment_info_get_fragment_name_v2 instead.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* name;
 * tiledb_fragment_info_get_fragment_name(ctx, fragment_info, 1, &name);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param fragment_info The fragment info object.
 * @param fid The index of the fragment of interest.
 * @param name The fragment name to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_fragment_info_get_fragment_name(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char** name) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_DEPRECATED_H
