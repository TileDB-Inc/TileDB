/**
 * @file tiledb/api/c_api/array/array_api_external.h
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
 * This file declares the Array C API for TileDB.
 */

#ifndef TILEDB_CAPI_ARRAY_EXTERNAL_H
#define TILEDB_CAPI_ARRAY_EXTERNAL_H

#include "../api_external_common.h"
#include "../array_schema/array_schema_api_external.h"
#include "../context/context_api_external.h"
#include "../query/query_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/** C API carrier for a TileDB array */
typedef struct tiledb_array_handle_t tiledb_array_t;

/** Encryption type. */
typedef enum {
/** Helper macro for defining encryption enums. */
#define TILEDB_ENCRYPTION_TYPE_ENUM(id) TILEDB_##id
#include "tiledb/api/c_api/array/encryption_type_enum.h"
#undef TILEDB_ENCRYPTION_TYPE_ENUM
} tiledb_encryption_type_t;

/**
 * Returns a string representation of the given encryption type.
 *
 * @param[in] encryption_type Encryption type
 * @param[out] str A constant string representation of the encryption type
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_encryption_type_to_str(
    tiledb_encryption_type_t encryption_type, const char** str) TILEDB_NOEXCEPT;

/**
 * Parses a encryption type from the given string.
 *
 * @param[in] str String representation to parse
 * @param[out] encryption_type The parsed encryption type
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_encryption_type_from_str(
    const char* str, tiledb_encryption_type_t* encryption_type) TILEDB_NOEXCEPT;

/**
 * Retrieves the latest schema of an array from the disk, creating an array
 * schema struct.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_t* array_schema;
 * tiledb_array_schema_load(ctx, "s3://tiledb_bucket/my_array", &array_schema);
 * // Make sure to free the array schema in the end
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_uri The array whose schema will be retrieved.
 * @param[out] array_schema The array schema to be retrieved, or `NULL` upon
 * error.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_load(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_t** array_schema) TILEDB_NOEXCEPT;

/**
 * Retrieves the latest schema of an array, creating an array schema struct.
 * Options to load additional features are read from the provided
 * tiledb_config_t* instance. If the provided config is nullptr, the config from
 * `ctx` is used instead.
 *
 * Currently supported options to be read from the config:
 *  - rest.load_enumerations_on_array_open - boolean
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_t* array_schema;
 * tiledb_array_schema_load_with_config(
 *     ctx,
 *     config,
 *     "s3://tiledb_bucket/my_array",
 *     &array_schema);
 * // Make sure to free the array schema in the end
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param config The TileDB config.
 * @param array_uri The array whose schema will be retrieved.
 * @param array_schema The array schema to be retrieved, or `NULL` upon error.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_load_with_config(
    tiledb_ctx_t* ctx,
    tiledb_config_t* config,
    const char* array_uri,
    tiledb_array_schema_t** array_schema) TILEDB_NOEXCEPT;

/**
 * Allocates a TileDB array object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "hdfs:///tiledb_arrays/my_array", &array);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_uri The array URI.
 * @param[out] array The array object to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_alloc(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_t** array) TILEDB_NOEXCEPT;

/**
 * Frees a TileDB array object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "hdfs:///tiledb_arrays/my_array", &array);
 * tiledb_array_open(ctx, array, TILEDB_READ);
 * tiledb_array_close(ctx, array);
 * tiledb_array_free(&array);
 * @endcode
 *
 * @param[in] array The array object to be freed.
 */
TILEDB_EXPORT void tiledb_array_free(tiledb_array_t** array) TILEDB_NOEXCEPT;

/**
 * Creates a new TileDB array given an input schema.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_create(ctx, "hdfs:///tiledb_arrays/my_array", array_schema);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_uri The array name.
 * @param[in] array_schema The array schema.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_create(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const tiledb_array_schema_t* array_schema) TILEDB_NOEXCEPT;

/**
 * Opens a TileDB array. The array is opened using a query type as input.
 * This is to indicate that queries created for this `tiledb_array_t`
 * object will inherit the query type. In other words, `tiledb_array_t`
 * objects are opened to receive only one type of queries.
 * They can always be closed and be re-opened with another query type.
 * Also there may be many different `tiledb_array_t`
 * objects created and opened with different query types.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "hdfs:///tiledb_arrays/my_array", &array);
 * tiledb_array_open(ctx, array, TILEDB_READ);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array The array object to be opened.
 * @param[in] query_type The type of queries the array object will be receiving.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note If the same array object is opened again without being closed,
 *     an error will be thrown.
 * @note The config should be set before opening an array.
 * @note If the array is to be opened at a specfic time interval, the
 *      `timestamp{start, end}` values should be set to a config that's set to
 *       the array object before opening the array.
 */
TILEDB_EXPORT capi_return_t tiledb_array_open(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_query_type_t query_type) TILEDB_NOEXCEPT;

/**
 * Checks if the array is open.
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array The array to be checked.
 * @param[out] is_open `1` if the array is open and `0` otherwise.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_is_open(
    tiledb_ctx_t* ctx, tiledb_array_t* array, int32_t* is_open) TILEDB_NOEXCEPT;

/**
 * Closes a TileDB array.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "hdfs:///tiledb_arrays/my_array", &array);
 * tiledb_array_open(ctx, array, TILEDB_READ);
 * tiledb_array_close(ctx, array);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array The array object to be closed.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note If the array object has already been closed, the function has
 *     no effect.
 */
TILEDB_EXPORT capi_return_t
tiledb_array_close(tiledb_ctx_t* ctx, tiledb_array_t* array) TILEDB_NOEXCEPT;

/**
 * Reopens a TileDB array (the array must be already open). This is useful
 * when the array got updated after it got opened and the `tiledb_array_t`
 * object got created. To sync-up with the updates, the user must either
 * close the array and open with `tiledb_array_open`, or just use
 * `tiledb_array_reopen` without closing. This function will be generally
 * faster than the former alternative.
 *
 * Note: reopening encrypted arrays does not require the encryption key.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "hdfs:///tiledb_arrays/my_array", &array);
 * tiledb_array_open(ctx, array, TILEDB_READ);
 * tiledb_array_reopen(ctx, array);
 *
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array The array object to be re-opened.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note This is applicable only to arrays opened for reads.
 * @note If the array is to be reopened after opening at a specfic time
 *      interval, the `timestamp{start, end}` values and subsequent config
 *      object should be reset for the array before reopening.
 */
TILEDB_EXPORT capi_return_t
tiledb_array_reopen(tiledb_ctx_t* ctx, tiledb_array_t* array) TILEDB_NOEXCEPT;

/**
 * Deletes all written array data.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_delete(ctx, "hdfs:///temp/my_array");
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] uri The Array's URI.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t
tiledb_array_delete(tiledb_ctx_t* ctx, const char* uri) TILEDB_NOEXCEPT;

/**
 * Deletes array fragments written between the input timestamps.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_delete_fragments_v2(
 *   ctx, "hdfs:///temp/my_array", 0, UINT64_MAX);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] uri_str The URI of the fragments' parent Array.
 * @param[in] timestamp_start The epoch timestamp in milliseconds.
 * @param[in] timestamp_end The epoch timestamp in milliseconds. Use UINT64_MAX
 * for the current timestamp.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_delete_fragments_v2(
    tiledb_ctx_t* ctx,
    const char* uri_str,
    uint64_t timestamp_start,
    uint64_t timestamp_end) TILEDB_NOEXCEPT;

/**
 * Deletes array fragments with the input uris.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* fragment_uris[2] = {
 *   "hdfs:///temp/my_array/__fragments/1",
 *   "hdfs:///temp/my_array/__fragments/2"};
 * tiledb_array_delete_fragments_list(
 *   ctx, "hdfs:///temp/my_array", fragment_uris, 2);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] uri_str The URI of the fragments' parent Array.
 * @param[in] fragment_uris The URIs of the fragments to be deleted.
 * @param[in] num_fragments The number of fragments to be deleted.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_delete_fragments_list(
    tiledb_ctx_t* ctx,
    const char* uri_str,
    const char* fragment_uris[],
    const size_t num_fragments) TILEDB_NOEXCEPT;

/**
 * Sets the array config.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "s3://tiledb_bucket/my_array", &array);
 * // Set the config for the given array.
 * tiledb_config_t* config;
 * tiledb_array_set_config(ctx, array, config);
 * tiledb_array_open(ctx, array, TILEDB_READ);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array The array to set the config for.
 * @param[in] config The config to be set.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note The config should be set before opening an array.
 */
TILEDB_EXPORT capi_return_t tiledb_array_set_config(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_config_t* config) TILEDB_NOEXCEPT;

/**
 * Sets the starting timestamp to use when opening (and reopening) the array.
 * This is an inclusive bound. The default value is `0`.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "s3://tiledb_bucket/my_array", &array);
 * tiledb_array_set_open_timestamp_start(ctx, array, 1234);
 * tiledb_array_open(ctx, array, TILEDB_READ);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array The array to set the timestamp on.
 * @param[in] timestamp_start The epoch timestamp in milliseconds.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_set_open_timestamp_start(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t timestamp_start) TILEDB_NOEXCEPT;

/**
 * Sets the ending timestamp to use when opening (and reopening) the array.
 * This is an inclusive bound. The default value is `UINT64_MAX`.
 *
 * When opening the array for reads, this value will be used to determine the
 * ending timestamp of fragments to load. A value of `UINT64_MAX` will be
 * translated to the current timestamp at which the array is opened.
 *
 * When opening the array for writes, this value will be used to determine the
 * timestamp to write new fragments. A value of `UINT64_MAX` will use the
 * current timestamp at which the array is opened. A value of 0 will use the
 * current timestamp at which the fragment is written.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "s3://tiledb_bucket/my_array", &array);
 * tiledb_array_set_open_timestamp_end(ctx, array, 5678);
 * tiledb_array_open(ctx, array, TILEDB_READ);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array The array to set the timestamp on.
 * @param[in] timestamp_end The epoch timestamp in milliseconds. Use UINT64_MAX
 * for the current timestamp.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_set_open_timestamp_end(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t timestamp_end) TILEDB_NOEXCEPT;

/**
 * Gets the array config.
 *
 * **Example:**
 *
 * @code{.c}
 * // Retrieve the config for the given array.
 * tiledb_config_t* config;
 * tiledb_array_get_config(ctx, array, config);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array The array to set the config for.
 * @param[out] config Set to the retrieved config.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_get_config(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_config_t** config) TILEDB_NOEXCEPT;

/**
 * Gets the starting timestamp used when opening (and reopening) the array.
 * This is an inclusive bound.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "s3://tiledb_bucket/my_array", &array);
 * tiledb_array_set_open_timestamp_start(ctx, array, 1234);
 * tiledb_array_open(ctx, array, TILEDB_READ);
 *
 * uint64_t timestamp_start;
 * tiledb_array_get_open_timestamp_start(ctx, array, &timestamp_start);
 * assert(timestamp_start == 1234);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array The array to set the timestamp on.
 * @param[out] timestamp_start The output epoch timestamp in milliseconds.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_get_open_timestamp_start(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t* timestamp_start) TILEDB_NOEXCEPT;

/**
 * Gets the ending timestamp used when opening (and reopening) the array.
 * This is an inclusive bound. If UINT64_MAX was set, this will return
 * the timestamp at the time the array was opened. If the array has not
 * yet been opened, it will return UINT64_MAX.`
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "s3://tiledb_bucket/my_array", &array);
 * tiledb_array_set_open_timestamp_end(ctx, array, 5678);
 * tiledb_array_open(ctx, array, TILEDB_READ);
 *
 * uint64_t timestamp_end;
 * tiledb_array_get_open_timestamp_end(ctx, array, &timestamp_end);
 * assert(timestamp_start == 5678);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array The array to set the timestamp on.
 * @param[out] timestamp_end The output epoch timestamp in milliseconds.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_get_open_timestamp_end(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t* timestamp_end) TILEDB_NOEXCEPT;

/**
 * Retrieves the schema of an array.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_t* array_schema;
 * tiledb_array_get_schema(ctx, array, &array_schema);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array The open array.
 * @param[out] array_schema The array schema to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 *
 * @note The user must free the array schema with `tiledb_array_schema_free`.
 */
TILEDB_EXPORT capi_return_t tiledb_array_get_schema(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_array_schema_t** array_schema) TILEDB_NOEXCEPT;

/**
 * Retrieves the query type with which the array was opened.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "hdfs:///tiledb_arrays/my_array", &array);
 * tiledb_array_open(ctx, array, TILEDB_READ);
 * tiledb_query_type_t query_type;
 * tiledb_array_get_type(ctx, array, &query_type);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array The array.
 * @param[out] query_type The query type to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_get_query_type(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_query_type_t* query_type) TILEDB_NOEXCEPT;

/**
 * Retrieves the URI the array was opened with. It outputs an error
 * if the array is not open.
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array The input array.
 * @param[out] array_uri The array URI to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_get_uri(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char** array_uri) TILEDB_NOEXCEPT;

/**
 * Upgrades an array to the latest format version.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* array_uri="test_array";
 * tiledb_array_upgrade_version(ctx, array_uri);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_uri The uri of the array.
 * @param[in] config Configuration parameters for the upgrade
 *     (`nullptr` means default, which will use the config from `ctx`).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_upgrade_version(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_config_t* config) TILEDB_NOEXCEPT;

/**
 * Retrieves the non-empty domain from an array. This is the union of the
 * non-empty domains of the array fragments. This API only works for arrays that
 * have numeric dimensions and all dimensions of the same type.
 *
 * The domain passed in is memory that can contain the number of dimensions * 2
 * * sizeof(type of dimension). The previous two is because we return the non
 * empty domain as min/max. Dimensions are returned in order.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t domain[4]; // Assuming a 2D array, 2 [low, high] pairs
 * int32_t is_empty;
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "my_array", &array);
 * tiledb_array_open(ctx, array, TILEDB_READ);
 * tiledb_array_get_non_empty_domain(ctx, array, domain, &is_empty);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] array The array object (must be opened beforehand).
 * @param[out] domain The domain to be retrieved.
 * @param[out] is_empty The function sets it to `1` if the non-empty domain is
 *     empty (i.e., the array does not contain any data yet), and `0` otherwise.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_get_non_empty_domain(
    tiledb_ctx_t* ctx, tiledb_array_t* array, void* domain, int32_t* is_empty)
    TILEDB_NOEXCEPT;

/**
 * Retrieves the non-empty domain from an array for a given dimension index.
 * This is the union of the non-empty domains of the array fragments on
 * the given dimension.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t domain[2];
 * int32_t is_empty;
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "my_array", &array);
 * tiledb_array_open(ctx, array, TILEDB_READ);
 * tiledb_array_get_non_empty_domain_from_index(ctx, array, 0, domain,
 * &is_empty);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] array The array object (must be opened beforehand).
 * @param[in] idx The dimension index, following the order as it was defined
 *      in the domain of the array schema.
 * @param[out] domain The domain to be retrieved.
 * @param[out] is_empty The function sets it to `1` if the non-empty domain is
 *     empty (i.e., the array does not contain any data yet), and `0` otherwise.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_get_non_empty_domain_from_index(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint32_t idx,
    void* domain,
    int32_t* is_empty) TILEDB_NOEXCEPT;

/**
 * Retrieves the non-empty domain from an array for a given dimension name.
 * This is the union of the non-empty domains of the array fragments on
 * the given dimension.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t domain[2];
 * int32_t is_empty;
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "my_array", &array);
 * tiledb_array_open(ctx, array, TILEDB_READ);
 * tiledb_array_get_non_empty_domain_from_name(ctx, array, "d1", domain,
 * &is_empty);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] array The array object (must be opened beforehand).
 * @param[in] name The dimension name.
 * @param[out] domain The domain to be retrieved.
 * @param[out] is_empty The function sets it to `1` if the non-empty domain is
 *     empty (i.e., the array does not contain any data yet), and `0` otherwise.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_get_non_empty_domain_from_name(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* name,
    void* domain,
    int32_t* is_empty) TILEDB_NOEXCEPT;

/**
 * Retrieves the non-empty domain range sizes from an array for a given
 * dimension index. This is the union of the non-empty domains of the array
 * fragments on the given dimension. Applicable only to var-sized dimensions.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t is_empty;
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "my_array", &array);
 * tiledb_array_open(ctx, array, TILEDB_READ);
 * uint64_t start_size, end_size;
 * tiledb_array_get_non_empty_domain_var_size_from_index(
 *     ctx, array, 0, &start_size, &end_size, &is_empty);
 * // If non-empty domain range is `[aa, dddd]`, then `start_size = 2`
 * // and `end_size = 4`.
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] array The array object (must be opened beforehand).
 * @param[in] idx The dimension index, following the order as it was defined
 *      in the domain of the array schema.
 * @param[out] start_size The size in bytes of the start range.
 * @param[out] end_size The size in bytes of the end range.
 * @param[out] is_empty The function sets it to `1` if the non-empty domain is
 *     empty (i.e., the array does not contain any data yet), and `0` otherwise.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t
tiledb_array_get_non_empty_domain_var_size_from_index(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint32_t idx,
    uint64_t* start_size,
    uint64_t* end_size,
    int32_t* is_empty) TILEDB_NOEXCEPT;

/**
 * Retrieves the non-empty domain range sizes from an array for a given
 * dimension name. This is the union of the non-empty domains of the array
 * fragments on the given dimension. Applicable only to var-sized dimensions.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t is_empty;
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "my_array", &array);
 * tiledb_array_open(ctx, array, TILEDB_READ);
 * uint64_t start_size, end_size;
 * tiledb_array_get_non_empty_domain_var_size_from_name(
 *     ctx, array, "d", &start_size, &end_size, &is_empty);
 * // If non-empty domain range is `[aa, dddd]`, then `start_size = 2`
 * // and `end_size = 4`.
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] array The array object (must be opened beforehand).
 * @param[in] name The dimension name.
 * @param[out] start_size The size in bytes of the start range.
 * @param[out] end_size The size in bytes of the end range.
 * @param[out] is_empty The function sets it to `1` if the non-empty domain is
 *     empty (i.e., the array does not contain any data yet), and `0` otherwise.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t
tiledb_array_get_non_empty_domain_var_size_from_name(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* name,
    uint64_t* start_size,
    uint64_t* end_size,
    int32_t* is_empty) TILEDB_NOEXCEPT;

/**
 * Retrieves the non-empty domain from an array for a given
 * dimension index. This is the union of the non-empty domains of the array
 * fragments on the given dimension. Applicable only to var-sized dimensions.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t is_empty;
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "my_array", &array);
 * tiledb_array_open(ctx, array, TILEDB_READ);
 *
 * // Get range sizes first
 * uint64_t start_size, end_size;
 * tiledb_array_get_non_empty_domain_var_size_from_index(
 *     ctx, array, 0, &start_size, &end_size, &is_empty);
 *
 * // Get domain
 * char start[start_size];
 * char end[end_size];
 * tiledb_array_get_non_empty_domain_var_from_index(
 *     ctx, array, 0, start, end, &is_empty);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] array The array object (must be opened beforehand).
 * @param[in] idx The dimension index, following the order as it was defined
 *      in the domain of the array schema.
 * @param[out] start The domain range start to set.
 * @param[out] end The domain range end to set.
 * @param[out] is_empty The function sets it to `1` if the non-empty domain is
 *     empty (i.e., the array does not contain any data yet), and `0` otherwise.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_get_non_empty_domain_var_from_index(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint32_t idx,
    void* start,
    void* end,
    int32_t* is_empty) TILEDB_NOEXCEPT;

/**
 * Retrieves the non-empty domain from an array for a given
 * dimension name. This is the union of the non-empty domains of the array
 * fragments on the given dimension. Applicable only to var-sized dimensions.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t domain[2];
 * int32_t is_empty;
 * tiledb_array_t* array;
 * tiledb_array_alloc(ctx, "my_array", &array);
 * tiledb_array_open(ctx, array, TILEDB_READ);
 *
 * // Get range sizes first
 * uint64_t start_size, end_size;
 * tiledb_array_get_non_empty_domain_var_size_from_name(
 *     ctx, array, "d", &start_size, &end_size, &is_empty);
 *
 * // Get domain
 * char start[start_size];
 * char end[end_size];
 * tiledb_array_get_non_empty_domain_var_from_name(
 *     ctx, array, "d", start, end, &is_empty);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] array The array object (must be opened beforehand).
 * @param[in] name The dimension name.
 * @param[out] start The domain range start to set.
 * @param[out] end The domain range end to set.
 * @param[out] is_empty The function sets it to `1` if the non-empty domain is
 *     empty (i.e., the array does not contain any data yet), and `0` otherwise.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_get_non_empty_domain_var_from_name(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* name,
    void* start,
    void* end,
    int32_t* is_empty) TILEDB_NOEXCEPT;

/**
 * Retrieves the encryption type the array at the given URI was created with.
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_uri The array URI.
 * @param[out] encryption_type The array encryption type to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_encryption_type(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_encryption_type_t* encryption_type) TILEDB_NOEXCEPT;

/**
 * It puts a metadata key-value item to an open array. The array must
 * be opened in WRITE mode, otherwise the function will error out.
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array An array opened in WRITE mode.
 * @param[in] key The key of the metadata item to be added. UTF-8 encodings
 *     are acceptable.
 * @param[in] value_type The datatype of the value.
 * @param[in] value_num The value may consist of more than one items of the
 *     same datatype. This argument indicates the number of items in the
 *     value component of the metadata.
 * @param[in] value The metadata value in binary form.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note The writes will take effect only upon closing the array.
 */
TILEDB_EXPORT capi_return_t tiledb_array_put_metadata(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* key,
    tiledb_datatype_t value_type,
    uint32_t value_num,
    const void* value) TILEDB_NOEXCEPT;

/**
 * It deletes a metadata key-value item from an open array. The array must
 * be opened in WRITE mode, otherwise the function will error out.
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array An array opened in WRITE mode.
 * @param[in] key The key of the metadata item to be deleted.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note The writes will take effect only upon closing the array.
 *
 * @note If the key does not exist, this will take no effect
 *     (i.e., the function will not error out).
 */
TILEDB_EXPORT capi_return_t tiledb_array_delete_metadata(
    tiledb_ctx_t* ctx, tiledb_array_t* array, const char* key) TILEDB_NOEXCEPT;

/**
 * It gets a metadata key-value item from an open array. The array must
 * be opened in READ mode, otherwise the function will error out.
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array An array opened in READ mode.
 * @param[in] key The key of the metadata item to be retrieved. UTF-8 encodings
 *     are acceptable.
 * @param[out] value_type The datatype of the value.
 * @param[out] value_num The value may consist of more than one items of the
 *     same datatype. This argument indicates the number of items in the
 *     value component of the metadata. Keys with empty values are indicated
 *     by value_num == 1 and value == NULL.
 * @param[out] value The metadata value in binary form.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note If the key does not exist, then `value` will be NULL.
 */
TILEDB_EXPORT capi_return_t tiledb_array_get_metadata(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* key,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) TILEDB_NOEXCEPT;

/**
 * It gets then number of metadata items in an open array. The array must
 * be opened in READ mode, otherwise the function will error out.
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array An array opened in READ mode.
 * @param[out] num The number of metadata items to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_get_metadata_num(
    tiledb_ctx_t* ctx, tiledb_array_t* array, uint64_t* num) TILEDB_NOEXCEPT;

/**
 * It gets a metadata item from an open array using an index.
 * The array must be opened in READ mode, otherwise the function will
 * error out.
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array An array opened in READ mode.
 * @param[in] index The index used to get the metadata.
 * @param[out] key The metadata key.
 * @param[out] key_len The metadata key length.
 * @param[out] value_type The datatype of the value.
 * @param[out] value_num The value may consist of more than one items of the
 *     same datatype. This argument indicates the number of items in the
 *     value component of the metadata.
 * @param[out] value The metadata value in binary form.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_get_metadata_from_index(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t index,
    const char** key,
    uint32_t* key_len,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) TILEDB_NOEXCEPT;

/**
 * Checks whether a key exists in metadata from an open array. The array must
 * be opened in READ mode, otherwise the function will error out.
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array An array opened in READ mode.
 * @param[in] key The key to be checked. UTF-8 encoding are acceptable.
 * @param[out] value_type The datatype of the value, if any.
 * @param[out] has_key Set to `1` if the metadata with given key exists, else
 * `0`.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note If the key does not exist, then `value` will be NULL.
 */
TILEDB_EXPORT capi_return_t tiledb_array_has_metadata_key(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* key,
    tiledb_datatype_t* value_type,
    int32_t* has_key) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_ARRAY_EXTERNAL_H
