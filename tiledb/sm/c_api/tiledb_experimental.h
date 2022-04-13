/**
 * @file   tiledb_experimental.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file declares experimental C API for TileDB.
 * Experimental APIs to do not fall under the API compatibility guarantees and
 * might change between TileDB versions
 */

#ifndef TILEDB_EXPERIMENTAL_H
#define TILEDB_EXPERIMENTAL_H

#include "tiledb.h"

/* ********************************* */
/*               MACROS              */
/* ********************************* */

#ifdef __cplusplus
extern "C" {
#endif

/** A TileDB array schema. */
typedef struct tiledb_array_schema_evolution_t tiledb_array_schema_evolution_t;

/* ********************************* */
/*      ARRAY SCHEMA EVOLUTION       */
/* ********************************* */

/**
 * Creates a TileDB schema evolution object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_evolution_t* array_schema_evolution;
 * tiledb_array_schema_evolution_alloc(ctx, &array_schema_evolution);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The TileDB schema evolution to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_alloc(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t** array_schema_evolution);

/**
 * Destroys an array schema evolution, freeing associated memory.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_evolution_free(&array_schema_evolution);
 * @endcode
 *
 * @param array_schema_evolution The array schema evolution to be destroyed.
 */
TILEDB_EXPORT void tiledb_array_schema_evolution_free(
    tiledb_array_schema_evolution_t** array_schema_evolution);

/**
 * Adds an attribute to an array schema evolution.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_attribute_t* attr;
 * tiledb_attribute_alloc(ctx, "my_attr", TILEDB_INT32, &attr);
 * tiledb_array_schema_evolution_add_attribute(ctx, array_schema_evolution,
 * attr);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param attr The attribute to be added.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_attribute_t* attribute);

/**
 * Drops an attribute to an array schema evolution.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* attribute_name="a1";
 * tiledb_array_schema_evolution_drop_attribute(ctx, array_schema_evolution,
 * attribute_name);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param attribute_name The name of the attribute to be dropped.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_drop_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    const char* attribute_name);

/**
 * Sets timestamp range in an array schema evolution
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t timestamp = tiledb_timestamp_now_ms();
 * tiledb_array_schema_evolution_set_timestamp_range(ctx,
 * array_schema_evolution, timestamp, timestamp);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param attribute_name The name of the attribute to be dropped.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_set_timestamp_range(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    uint64_t lo,
    uint64_t hi);

/* ********************************* */
/*          ARRAY SCHEMA             */
/* ********************************* */

TILEDB_EXPORT int32_t tiledb_array_schema_timestamp_range(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    uint64_t* lo,
    uint64_t* hi);

/* ********************************* */
/*               ARRAY               */
/* ********************************* */

/**
 * Evolve array schema of an array.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* array_uri="test_array";
 * tiledb_array_evolve(ctx, array_uri,array_schema_evolution);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_uri The uri of the array.
 * @param array_schema_evolution The schema evolution.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_evolve(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_evolution_t* array_schema_evolution);

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
 * @param ctx The TileDB context.
 * @param array_uri The uri of the array.
 * @param config Configuration parameters for the upgrade
 *     (`nullptr` means default, which will use the config from `ctx`).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_upgrade_version(
    tiledb_ctx_t* ctx, const char* array_uri, tiledb_config_t* config);

/* ********************************* */
/*               QUERY               */
/* ********************************* */

/**
 * Adds point ranges to the given dimension index of the subarray
 * Effectively `add_range(x_i, x_i)` for `count` points in the
 * target array, but set in bulk to amortize expensive steps.
 */
TILEDB_EXPORT int32_t tiledb_subarray_add_point_ranges(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const void* start,
    uint64_t count);

/**
 * Adds a set of point ranges along subarray dimension index. Each value
 * in the target array is added as `add_range(x,x)` for count elements.
 * The datatype of the range components must be the same as the type of
 * the dimension of the array in the query.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t dim_idx = 2;
 * int64_t ranges[] = { 20, 21, 25, 31}
 * tiledb_query_add_point_ranges(ctx, query, dim_idx, &ranges, 4);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query to add the range to.
 * @param dim_idx The index of the dimension to add the range to.
 * @param start The start of the ranges array.
 * @param count Number of ranges to add.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note The stride is currently unsupported. Use `nullptr` as the
 *     stride argument.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_add_point_ranges(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    uint32_t dim_idx,
    const void* start,
    uint64_t count);

/* ********************************* */
/*        QUERY STATUS DETAILS       */
/* ********************************* */

/** This should move to c_api/tiledb.h when stabilized */
typedef struct tiledb_query_status_details_t tiledb_query_status_details_t;

/** TileDB query status details type. */
typedef enum {
/** Helper macro for defining status details type enums. */
#define TILEDB_QUERY_STATUS_DETAILS_ENUM(id) TILEDB_##id
#include "tiledb_enum.h"
#undef TILEDB_QUERY_STATUS_DETAILS_ENUM
} tiledb_query_status_details_reason_t;

/** This should move to c_api/tiledb_struct_defs.h when stabilized */
struct tiledb_query_status_details_t {
  tiledb_query_status_details_reason_t incomplete_reason;
};

/**
 * Get extended query status details.
 *
 * The contained enumeration tiledb_query_status_details_reason_t
 * indicates extended information about a returned query status
 * in order to allow improved client-side handling of buffers and
 * potential resubmissions.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_status_details_t status_details;
 * tiledb_query_get_status_details(ctx, query, &status_details);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query from which to retrieve status details.
 * @param status_details The tiledb_query_status_details_t struct.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_status_details(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_query_status_details_t* status);

/* ********************************* */
/*              CONTEXT              */
/* ********************************* */

/**
 * Creates a TileDB context, which contains the TileDB storage manager
 * that manages everything in the TileDB library. This is a provisional API
 * which returns an error object when the context creation fails. This API will
 * be replaced with a more proper "v2" of context alloc in the near future. The
 * main goal is to use to this to capture potential failures to inform the v2
 * alloc design.
 *
 * **Examples:**
 *
 * Without config (i.e., use default configuration):
 *
 * @code{.c}
 * tiledb_ctx_t* ctx;
 * tiledb_error_t* error;
 * tiledb_ctx_alloc_with_error(NULL, &ctx, &error);
 * @endcode
 *
 * With some config:
 *
 * @code{.c}
 * tiledb_ctx_t* ctx;
 * tiledb_error_t* error;
 * tiledb_ctx_alloc_with_error(config, &ctx, &error);
 * @endcode
 *
 * @param[in] config The configuration parameters (`NULL` means default).
 * @param[out] ctx The TileDB context to be created.
 * @param[out] error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_ctx_alloc_with_error(
    tiledb_config_t* config, tiledb_ctx_t** ctx, tiledb_error_t** error);

/* ********************************* */
/*                GROUP              */
/* ********************************* */

/**
 * Creates a new TileDB group.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_group_t* group;
 * tiledb_group_alloc(ctx, "my_group", &group);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group_uri The group URI.
 * @param group The TileDB group to be allocated
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_group_alloc(
    tiledb_ctx_t* ctx, const char* group_uri, tiledb_group_t** group);

/**
 * Opens a TileDB group. The group is opened using a query type as input.
 * This is to indicate that queries created for this `tiledb_group_t`
 * object will inherit the query type. In other words, `tiledb_group_t`
 * objects are opened to receive only one type of queries.
 * They can always be closed and be re-opened with another query type.
 * Also there may be many different `tiledb_group_t`
 * objects created and opened with different query types.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_group_t* group;
 * tiledb_group_alloc(ctx, "hdfs:///tiledb_groups/my_group", &group);
 * tiledb_group_open(ctx, group, TILEDB_READ);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group The group object to be opened.
 * @param query_type The type of queries the group object will be receiving.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note If the same group object is opened again without being closed,
 *     an error will be set and TILEDB_ERR returned.
 * @note The config should be set before opening an group.
 * @note If the group is to be opened at a specfic time interval, the
 *      `timestamp{start, end}` values should be set to a config that's set to
 *       the group object before opening the group.
 */
TILEDB_EXPORT int32_t tiledb_group_open(
    tiledb_ctx_t* ctx, tiledb_group_t* group, tiledb_query_type_t query_type);

/**
 * Closes a TileDB group.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_group_t* group;
 * tiledb_group_alloc(ctx, "hdfs:///tiledb_groups/my_group", &group);
 * tiledb_group_open(ctx, group, TILEDB_READ);
 * tiledb_group_close(ctx, group);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group The group object to be closed.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note If the group object has already been closed, the function has
 *     no effect.
 */
TILEDB_EXPORT int32_t
tiledb_group_close(tiledb_ctx_t* ctx, tiledb_group_t* group);

/**
 * Creates a new TileDB group.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_group_t* group;
 * tiledb_group_alloc(ctx, "my_group", &group);
 * tiledb_group_free(&group);
 * @endcode
 *
 * @param group The TileDB group to be freed
 */
TILEDB_EXPORT void tiledb_group_free(tiledb_group_t** group);

/**
 * Sets the group config.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_group_t* group;
 * tiledb_group_alloc(ctx, "s3://tiledb_bucket/my_group", &group);
 * tiledb_group_open(ctx, group, TILEDB_READ);
 * // Set the config for the given group.
 * tiledb_config_t* config;
 * tiledb_group_set_config(ctx, group, config);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group The group to set the config for.
 * @param config The config to be set.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note The group does not need to be opened via `tiledb_group_open_at` to use
 *      this function.
 * @note The config should be set before opening an group.
 */
TILEDB_EXPORT int32_t tiledb_group_set_config(
    tiledb_ctx_t* ctx, tiledb_group_t* group, tiledb_config_t* config);

/**
 * Gets the group config.
 *
 * **Example:**
 *
 * @code{.c}
 * // Retrieve the config for the given group.
 * tiledb_config_t* config;
 * tiledb_group_get_config(ctx, group, config);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group The group to set the config for.
 * @param config Set to the retrieved config.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_group_get_config(
    tiledb_ctx_t* ctx, tiledb_group_t* group, tiledb_config_t** config);

/**
 * It puts a metadata key-value item to an open group. The group must
 * be opened in WRITE mode, otherwise the function will error out.
 *
 * @param ctx The TileDB context.
 * @param group An group opened in WRITE mode.
 * @param key The key of the metadata item to be added. UTF-8 encodings
 *     are acceptable.
 * @param value_type The datatype of the value.
 * @param value_num The value may consist of more than one items of the
 *     same datatype. This argument indicates the number of items in the
 *     value component of the metadata.
 * @param value The metadata value in binary form.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note The writes will take effect only upon closing the group.
 */
TILEDB_EXPORT int32_t tiledb_group_put_metadata(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key,
    tiledb_datatype_t value_type,
    uint32_t value_num,
    const void* value);

/**
 * It deletes a metadata key-value item from an open group. The group must
 * be opened in WRITE mode, otherwise the function will error out.
 *
 * @param ctx The TileDB context.
 * @param group An group opened in WRITE mode.
 * @param key The key of the metadata item to be deleted.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note The writes will take effect only upon closing the group.
 *
 * @note If the key does not exist, this will take no effect
 *     (i.e., the function will not error out).
 */
TILEDB_EXPORT int32_t tiledb_group_delete_metadata(
    tiledb_ctx_t* ctx, tiledb_group_t* group, const char* key);

/**
 * It gets a metadata key-value item from an open group. The group must
 * be opened in READ mode, otherwise the function will error out.
 *
 * @param ctx The TileDB context.
 * @param group An group opened in READ mode.
 * @param key The key of the metadata item to be retrieved. UTF-8 encodings
 *     are acceptable.
 * @param value_type The datatype of the value.
 * @param value_num The value may consist of more than one items of the
 *     same datatype. This argument indicates the number of items in the
 *     value component of the metadata. Keys with empty values are indicated
 *     by value_num == 1 and value == NULL.
 * @param value The metadata value in binary form.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note If the key does not exist, then `value` will be NULL.
 */
TILEDB_EXPORT int32_t tiledb_group_get_metadata(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value);

/**
 * It gets then number of metadata items in an open group. The group must
 * be opened in READ mode, otherwise the function will error out.
 *
 * @param ctx The TileDB context.
 * @param group An group opened in READ mode.
 * @param num The number of metadata items to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_group_get_metadata_num(
    tiledb_ctx_t* ctx, tiledb_group_t* group, uint64_t* num);

/**
 * It gets a metadata item from an open group using an index.
 * The group must be opened in READ mode, otherwise the function will
 * error out.
 *
 * @param ctx The TileDB context.
 * @param group An group opened in READ mode.
 * @param index The index used to get the metadata.
 * @param key The metadata key.
 * @param key_len The metadata key length.
 * @param value_type The datatype of the value.
 * @param value_num The value may consist of more than one items of the
 *     same datatype. This argument indicates the number of items in the
 *     value component of the metadata.
 * @param value The metadata value in binary form.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_group_get_metadata_from_index(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    uint64_t index,
    const char** key,
    uint32_t* key_len,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value);

/**
 * Checks whether a key exists in metadata from an open group. The group must
 * be opened in READ mode, otherwise the function will error out.
 *
 * @param ctx The TileDB context.
 * @param group An group opened in READ mode.
 * @param key The key to be checked. UTF-8 encoding are acceptable.
 * @param value_type The datatype of the value, if any.
 * @param has_key Set to `1` if the metadata with given key exists, else `0`.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note If the key does not exist, then `value` will be NULL.
 */
TILEDB_EXPORT int32_t tiledb_group_has_metadata_key(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key,
    tiledb_datatype_t* value_type,
    int32_t* has_key);

/**
 * Add a member to a group
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_group_t* group;
 * tiledb_group_alloc(ctx, "s3://tiledb_bucket/my_group", &group);
 * tiledb_group_open(ctx, group, TILEDB_WRITE);
 * tiledb_group_add_member(ctx, group, "s3://tiledb_bucket/my_array");
 * tiledb_group_add_member(ctx, group, "s3://tiledb_bucket/my_group_2");
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group An group opened in WRITE mode.
 * @param uri URI of member to add
 * @param relative is the URI relative to the group
 * @param name optional name group member can be given to be looked up by. Set
 * to NULL if wishing to remain unset.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_group_add_member(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* uri,
    const uint8_t relative,
    const char* name);

/**
 * Remove a member from a group
 *
 * * @code{.c}
 * tiledb_group_t* group;
 * tiledb_group_alloc(ctx, "s3://tiledb_bucket/my_group", &group);
 * tiledb_group_open(ctx, group, TILEDB_WRITE);
 * tiledb_group_remove_member(ctx, group, "s3://tiledb_bucket/my_array");
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group An group opened in WRITE mode.
 * @param uri URI of member to remove. Passing a name is also supported if the
 * group member was assigned a name.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_group_remove_member(
    tiledb_ctx_t* ctx, tiledb_group_t* group, const char* uri);

/**
 * Get the count of members in a group
 *
 * * @code{.c}
 * tiledb_group_t* group;
 * tiledb_group_alloc(ctx, "s3://tiledb_bucket/my_group", &group);
 * tiledb_group_open(ctx, group, TILEDB_WRITE);
 * tiledb_group_add_member(ctx, group, "s3://tiledb_bucket/my_array");
 * tiledb_group_add_member(ctx, group, "s3://tiledb_bucket/my_group_2");
 *
 * tiledb_group_close(ctx, group);
 * tiledb_group_open(ctx, group, TILEDB_READ);
 * uint64_t count = 0;
 * tiledb_group_get_member_count(ctx, group, &count);
 *
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group An group opened in READ mode.
 * @param count number of members in group
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_group_get_member_count(
    tiledb_ctx_t* ctx, tiledb_group_t* group, uint64_t* count);

/**
 * Get a member of a group by index and details of group
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_group_t* group;
 * tiledb_group_alloc(ctx, "s3://tiledb_bucket/my_group", &group);
 * tiledb_group_open(ctx, group, TILEDB_WRITE);
 * tiledb_group_add_member(ctx, group, "s3://tiledb_bucket/my_array");
 * tiledb_group_add_member(ctx, group, "s3://tiledb_bucket/my_group_2");
 *
 * tiledb_group_close(ctx, group);
 * tiledb_group_open(ctx, group, TILEDB_READ);
 * char *uri;
 * tiledb_object_t type;
 * tiledb_group_get_member_by_index(ctx, group, 0, &uri, &type);
 *
 * free(uri);
 *
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group An group opened in READ mode.
 * @param index index of member to fetch
 * @param uri URI of member, The caller takes ownership
 *   of the c-string.
 * @param type type of member
 * @param name name of member, The caller takes ownership
 *   of the c-string. NULL if name was not set
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_group_get_member_by_index(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    uint64_t index,
    char** uri,
    tiledb_object_t* type,
    char** name);

/**
 * Get a member of a group by index and details of group
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_group_t* group;
 * tiledb_group_alloc(ctx, "s3://tiledb_bucket/my_group", &group);
 * tiledb_group_open(ctx, group, TILEDB_WRITE);
 * tiledb_group_add_member(ctx, group, "s3://tiledb_bucket/my_array", "array1");
 * tiledb_group_add_member(ctx, group, "s3://tiledb_bucket/my_group_2",
 * "group2");
 *
 * tiledb_group_close(ctx, group);
 * tiledb_group_open(ctx, group, TILEDB_READ);
 * char *uri;
 * tiledb_object_t type;
 * tiledb_group_get_member_by_name(ctx, group, "array1", &uri, &type);
 *
 * free(uri);
 *
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group An group opened in READ mode.
 * @param name name of member to fetch
 * @param uri URI of member, The caller takes ownership
 *   of the c-string.
 * @param type type of member
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_group_get_member_by_name(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* name,
    char** uri,
    tiledb_object_t* type);

/**
 * Checks if the group is open.
 *
 * @param ctx The TileDB context.
 * @param group The group to be checked.
 * @param is_open `1` if the group is open and `0` otherwise.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_group_is_open(
    tiledb_ctx_t* ctx, tiledb_group_t* group, int32_t* is_open);

/**
 * Retrieves the URI the group was opened with. It outputs an error
 * if the group is not open.
 *
 * @param ctx The TileDB context.
 * @param group The input group.
 * @param group_uri The group URI to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_group_get_uri(
    tiledb_ctx_t* ctx, tiledb_group_t* group, const char** group_uri);

/**
 * Retrieves the query type with which the group was opened.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_group_t* group;
 * tiledb_group_alloc(ctx, "s3://tiledb_groups/my_group", &group);
 * tiledb_group_open(ctx, group, TILEDB_READ);
 * tiledb_query_type_t query_type;
 * tiledb_group_get_type(ctx, group, &query_type);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group The group.
 * @param query_type The query type to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_group_get_query_type(
    tiledb_ctx_t* ctx, tiledb_group_t* group, tiledb_query_type_t* query_type);

/**
 * Dump a string representation of a group
 *
 * @param ctx The TileDB context.
 * @param group The group.
 * @param dump_ascii The output string. The caller takes ownership
 *   of the c-string.
 * @param recursive should we recurse into sub-groups
 * @return  `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_group_dump_str(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    char** dump_ascii,
    uint8_t recursive);

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_EXPERIMENTAL_H
