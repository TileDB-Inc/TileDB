/**
 * @file tiledb/api/c_api/group/group_api_external.h
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
 * This file declares the group C API for TileDB.
 */

#ifndef TILEDB_CAPI_GROUP_API_EXTERNAL_H
#define TILEDB_CAPI_GROUP_API_EXTERNAL_H

#include "../api_external_common.h"
#include "../datatype/datatype_api_external.h"
#include "../object/object_api_external.h"
#include "../query/query_api_external.h"
#include "group_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/** A group object. */
typedef struct tiledb_group_handle_t tiledb_group_t;

/**
 * Creates a new TileDB group.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_group_create(ctx, "my_group");
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group_uri The group URI.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t
tiledb_group_create(tiledb_ctx_t* ctx, const char* group_uri) TILEDB_NOEXCEPT;

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
TILEDB_EXPORT capi_return_t tiledb_group_alloc(
    tiledb_ctx_t* ctx,
    const char* group_uri,
    tiledb_group_t** group) TILEDB_NOEXCEPT;

/**
 * Destroys a TileDB group, freeing associated memory.
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
TILEDB_EXPORT void tiledb_group_free(tiledb_group_t** group) TILEDB_NOEXCEPT;

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
TILEDB_EXPORT capi_return_t tiledb_group_open(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_query_type_t query_type) TILEDB_NOEXCEPT;

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
TILEDB_EXPORT capi_return_t
tiledb_group_close(tiledb_ctx_t* ctx, tiledb_group_t* group) TILEDB_NOEXCEPT;

/**
 * Sets the group config.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_group_t* group;
 * tiledb_group_alloc(ctx, "s3://tiledb_bucket/my_group", &group);
 * // Set the config for the given group.
 * tiledb_config_t* config;
 * tiledb_group_set_config(ctx, group, config);
 * tiledb_group_open(ctx, group, TILEDB_READ);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group The group to set the config for.
 * @param config The config to be set.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @pre The config must be set on a closed group.
 */
TILEDB_EXPORT capi_return_t tiledb_group_set_config(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_config_t* config) TILEDB_NOEXCEPT;

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
TILEDB_EXPORT capi_return_t tiledb_group_get_config(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_config_t** config) TILEDB_NOEXCEPT;

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
TILEDB_EXPORT capi_return_t tiledb_group_put_metadata(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key,
    tiledb_datatype_t value_type,
    uint32_t value_num,
    const void* value) TILEDB_NOEXCEPT;

/**
 * Deletes written data from an open group. The group must
 * be opened in MODIFY_EXCLSUIVE mode, otherwise the function will error out.
 *
 * @param ctx The TileDB context.
 * @param group An group opened in MODIFY_EXCLUSIVE mode.
 * @param uri The address of the group item to be deleted.
 * @param recursive True if all data inside the group is to be deleted.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note if recursive == false, data added to the group will be left as-is.
 */
TILEDB_EXPORT int32_t tiledb_group_delete_group(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* uri,
    const uint8_t recursive) TILEDB_NOEXCEPT;

/**
 * Deletes a metadata key-value item from an open group. The group must
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
TILEDB_EXPORT capi_return_t tiledb_group_delete_metadata(
    tiledb_ctx_t* ctx, tiledb_group_t* group, const char* key) TILEDB_NOEXCEPT;

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
TILEDB_EXPORT capi_return_t tiledb_group_get_metadata(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) TILEDB_NOEXCEPT;

/**
 * It gets then number of metadata items in an open group. The group must
 * be opened in READ mode, otherwise the function will error out.
 *
 * @param ctx The TileDB context.
 * @param group An group opened in READ mode.
 * @param num The number of metadata items to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_group_get_metadata_num(
    tiledb_ctx_t* ctx, tiledb_group_t* group, uint64_t* num) TILEDB_NOEXCEPT;

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
TILEDB_EXPORT capi_return_t tiledb_group_get_metadata_from_index(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    uint64_t index,
    const char** key,
    uint32_t* key_len,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) TILEDB_NOEXCEPT;

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
TILEDB_EXPORT capi_return_t tiledb_group_has_metadata_key(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key,
    tiledb_datatype_t* value_type,
    int32_t* has_key) TILEDB_NOEXCEPT;

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
 * @param name optional name group member can be given to be looked up by.
 * Can be set to NULL.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_group_add_member(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* uri,
    const uint8_t relative,
    const char* name) TILEDB_NOEXCEPT;

/**
 * Add a member with a known type to a group
 * The caller should make sure the correct type is provided for the member being
 * added as this API will not check it for correctness in favor of efficiency
 * and results can be undefined otherwise.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_group_t* group;
 * tiledb_group_alloc(ctx, "s3://tiledb_bucket/my_group", &group);
 * tiledb_group_open(ctx, group, TILEDB_WRITE);
 * tiledb_group_add_member(ctx, group, "s3://tiledb_bucket/my_array",
 * TILEDB_ARRAY); tiledb_group_add_member(ctx, group,
 * "s3://tiledb_bucket/my_group_2", TILEDB_GROUP);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group An group opened in WRITE mode.
 * @param uri URI of member to add
 * @param relative is the URI relative to the group
 * @param name optional name group member can be given to be looked up by.
 * Can be set to NULL.
 *  @param type the type of the member getting added if known in advance.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_group_add_member_by_type(
    tiledb_group_handle_t* group,
    const char* group_uri,
    const uint8_t relative,
    const char* name,
    tiledb_object_t type) TILEDB_NOEXCEPT;

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
 * @param name_or_uri Name or URI of member to remove. If the URI is
 * registered multiple times in the group, the name needs to be specified so
 * that the correct one can be removed. Note that if a URI is registered as
 * both a named and unnamed member, the unnamed member will be removed
 * successfully using the URI.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_group_remove_member(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* name_or_uri) TILEDB_NOEXCEPT;

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
TILEDB_EXPORT capi_return_t tiledb_group_get_member_count(
    tiledb_ctx_t* ctx, tiledb_group_t* group, uint64_t* count) TILEDB_NOEXCEPT;

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
 * tiledb_string_t *uri, *name;
 * tiledb_object_t type;
 * tiledb_group_get_member_by_index_v2(ctx, group, 0, &uri, &type, &name);
 *
 * tiledb_string_free(uri);
 * tiledb_string_free(name);
 *
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group An group opened in READ mode.
 * @param index index of member to fetch
 * @param uri Handle to the URI of the member.
 * @param type type of member
 * @param name Handle to the name of the member. NULL if name was not set
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_group_get_member_by_index_v2(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    uint64_t index,
    tiledb_string_t** uri,
    tiledb_object_t* type,
    tiledb_string_t** name) TILEDB_NOEXCEPT;

/**
 * Get a member of a group by name and details of group.
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
 * tilledb_string_t *uri;
 * tiledb_object_t type;
 * tiledb_group_get_member_by_name_v2(ctx, group, "array1", &uri, &type);
 *
 * tiledb_string_free(uri);
 *
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group An group opened in READ mode.
 * @param name name of member to fetch
 * @param uri Handle to the URI of the member.
 * @param type type of member
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_group_get_member_by_name_v2(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* name,
    tiledb_string_t** uri,
    tiledb_object_t* type) TILEDB_NOEXCEPT;

/* (clang format was butchering the tiledb_group_add_member() calls) */
/* clang-format off */
/**
 * Get a member of a group by name and relative characteristic of that name
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_group_t* group;
 * tiledb_group_alloc(ctx, "s3://tiledb_bucket/my_group", &group);
 * tiledb_group_open(ctx, group, TILEDB_WRITE);
 * tiledb_group_add_member(ctx, group, "s3://tiledb_bucket/my_array", true,
 *     "array1");
 * tiledb_group_add_member(ctx, group, "s3://tiledb_bucket/my_group_2",
 *     false, "group2");
 *
 * tiledb_group_close(ctx, group);
 * tiledb_group_open(ctx, group, TILEDB_READ);
 * uint8_t is_relative;
 * tiledb_group_get_is_relative_uri_by_name(ctx, group, "array1", &is_relative);
 *
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] group An group opened in READ mode.
 * @param[in] name name of member to fetch
 * @param[out] is_relative to receive relative characteristic of named member
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
/* clang-format on */
TILEDB_EXPORT capi_return_t tiledb_group_get_is_relative_uri_by_name(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* name,
    uint8_t* relative) TILEDB_NOEXCEPT;

/**
 * Checks if the group is open.
 *
 * @param ctx The TileDB context.
 * @param group The group to be checked.
 * @param is_open `1` if the group is open and `0` otherwise.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_group_is_open(
    tiledb_ctx_t* ctx, tiledb_group_t* group, int32_t* is_open) TILEDB_NOEXCEPT;

/**
 * Retrieves the URI the group was opened with. It outputs an error
 * if the group is not open.
 *
 * @param ctx The TileDB context.
 * @param group The input group.
 * @param group_uri The group URI to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_group_get_uri(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char** group_uri) TILEDB_NOEXCEPT;

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
TILEDB_EXPORT capi_return_t tiledb_group_get_query_type(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_query_type_t* query_type) TILEDB_NOEXCEPT;

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
TILEDB_EXPORT capi_return_t tiledb_group_dump_str(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    char** dump_ascii,
    const uint8_t recursive) TILEDB_NOEXCEPT;

/**
 * Consolidates the group metadata into a single group metadata file.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_group_consolidate_metadata(
 *     ctx, "tiledb:///groups/mygroup", nullptr);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group_uri The name of the TileDB group whose metadata will
 *     be consolidated.
 * @param config Configuration parameters for the consolidation
 *     (`nullptr` means default, which will use the config from `ctx`).
 * @return `TILEDB_OK` on success, and `TILEDB_ERR` on error.
 */
TILEDB_EXPORT capi_return_t tiledb_group_consolidate_metadata(
    tiledb_ctx_t* ctx,
    const char* group_uri,
    tiledb_config_t* config) TILEDB_NOEXCEPT;

/**
 * Cleans up the group metadata
 * Note that this will coarsen the granularity of time traveling (see docs
 * for more information).
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_group_vacuum_metadata(
 *     ctx, "tiledb:///groups/mygroup", nullptr);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param group_uri The name of the TileDB group to vacuum.
 * @param config Configuration parameters for the vacuuming
 *     (`nullptr` means default, which will use the config from `ctx`).
 * @return `TILEDB_OK` on success, and `TILEDB_ERR` on error.
 */
TILEDB_EXPORT capi_return_t tiledb_group_vacuum_metadata(
    tiledb_ctx_t* ctx,
    const char* group_uri,
    tiledb_config_t* config) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_GROUP_API_EXTERNAL_H
