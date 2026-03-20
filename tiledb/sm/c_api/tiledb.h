/*
 * @file   tiledb.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 * This file declares the C API for TileDB.
 */

#ifndef TILEDB_H
#define TILEDB_H

#include "tiledb_version.h"

#ifdef TILEDB_NO_API_DEPRECATION_WARNINGS
// Define these before including tiledb_export.h to avoid their normal
// definitions.
#ifndef TILEDB_DEPRECATED
#define TILEDB_DEPRECATED
#endif
#ifndef TILEDB_DEPRECATED_EXPORT
#define TILEDB_DEPRECATED_EXPORT
#endif
#endif

/*
 * Common definitions for export, noexcept, etc.
 */
#include "tiledb/api/c_api/api_external_common.h"

/*
 * API sections
 */
#include "tiledb/api/c_api/array/array_api_external.h"
#include "tiledb/api/c_api/array_schema/array_schema_api_external.h"
#include "tiledb/api/c_api/array_schema_evolution/array_schema_evolution_api_experimental.h"
#include "tiledb/api/c_api/attribute/attribute_api_external.h"
#include "tiledb/api/c_api/buffer/buffer_api_external.h"
#include "tiledb/api/c_api/buffer_list/buffer_list_api_external.h"
#include "tiledb/api/c_api/config/config_api_external.h"
#include "tiledb/api/c_api/context/context_api_external.h"
#include "tiledb/api/c_api/data_order/data_order_api_external.h"
#include "tiledb/api/c_api/datatype/datatype_api_external.h"
#include "tiledb/api/c_api/dimension/dimension_api_external.h"
#include "tiledb/api/c_api/domain/domain_api_external.h"
#include "tiledb/api/c_api/error/error_api_external.h"
#include "tiledb/api/c_api/filesystem/filesystem_api_external.h"
#include "tiledb/api/c_api/filter/filter_api_external.h"
#include "tiledb/api/c_api/filter_list/filter_list_api_external.h"
#include "tiledb/api/c_api/fragment_info/fragment_info_api_external.h"
#include "tiledb/api/c_api/group/group_api_external.h"
#include "tiledb/api/c_api/object/object_api_external.h"
#include "tiledb/api/c_api/query/query_api_external.h"
#include "tiledb/api/c_api/string/string_api_external.h"
#include "tiledb/api/c_api/subarray/subarray_api_external.h"
#include "tiledb/api/c_api/vfs/vfs_api_external.h"

#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ****************************** */
/*          TILEDB ENUMS          */
/* ****************************** */

/** Query condition operator. */
typedef enum {
  /** Less-than operator */
  TILEDB_LT = 0,
  /** Less-than-or-equal operator */
  TILEDB_LE = 1,
  /** Greater-than operator */
  TILEDB_GT = 2,
  /** Greater-than-or-equal operator */
  TILEDB_GE = 3,
  /** Equal operator */
  TILEDB_EQ = 4,
  /** Not-equal operator */
  TILEDB_NE = 5,
  /** IN set membership operator. */
  TILEDB_IN = 6,
  /** NOT IN set membership operator. */
  TILEDB_NOT_IN = 7,
} tiledb_query_condition_op_t;

/** Query condition combination operator. */
typedef enum {
/** Helper macro for defining query condition combination operator enums. */
#define TILEDB_QUERY_CONDITION_COMBINATION_OP_ENUM(id) TILEDB_##id
#include "tiledb_enum.h"
#undef TILEDB_QUERY_CONDITION_COMBINATION_OP_ENUM
} tiledb_query_condition_combination_op_t;

/** MIME Type. */
typedef enum {
/** Helper macro for defining MimeType enums. */
#define TILEDB_MIME_TYPE_ENUM(id) TILEDB_##id
#include "tiledb_enum.h"
#undef TILEDB_MIME_TYPE_ENUM
} tiledb_mime_type_t;

/* ****************************** */
/*       ENUMS TO/FROM STR        */
/* ****************************** */

/**
 * Returns a string representation of the given query status.
 *
 * @param query_status Query status
 * @param str Set to point to a constant string representation of the query
 * status
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_status_to_str(
    tiledb_query_status_t query_status, const char** str) TILEDB_NOEXCEPT;

/**
 * Parses a query status from the given string.
 *
 * @param str String representation to parse
 * @param query_status Set to the parsed query status
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_status_from_str(
    const char* str, tiledb_query_status_t* query_status) TILEDB_NOEXCEPT;

/* ****************************** */
/*            CONSTANTS           */
/* ****************************** */

/** Returns a special value indicating a variable number of elements. */
TILEDB_EXPORT uint32_t tiledb_var_num(void) TILEDB_NOEXCEPT;

/** Returns the maximum path length on the current platform. */
TILEDB_EXPORT uint32_t tiledb_max_path(void) TILEDB_NOEXCEPT;

/**
 * Returns the size (in bytes) of an offset (used in variable-sized
 * attributes).
 */
TILEDB_EXPORT uint64_t tiledb_offset_size(void) TILEDB_NOEXCEPT;

/** Returns the current time in milliseconds. */
TILEDB_EXPORT uint64_t tiledb_timestamp_now_ms(void) TILEDB_NOEXCEPT;

/** Returns a special name indicating the timestamps attribute. */
TILEDB_EXPORT const char* tiledb_timestamps(void) TILEDB_NOEXCEPT;

/**
 * @name Constants wrapping special functions
 */
/**@{*/
/** A special value indicating a variable number of elements. */
#define TILEDB_VAR_NUM tiledb_var_num()
/** The maximum path length on the current platform. */
#define TILEDB_MAX_PATH tiledb_max_path()
/** The size (in bytes) of an offset (used in variable-sized attributes). */
#define TILEDB_OFFSET_SIZE tiledb_offset_size()
/** The current time in milliseconds. */
#define TILEDB_TIMESTAMP_NOW_MS tiledb_timestamp_now_ms()
/** A special name indicating the timestamps attribute. */
#define TILEDB_TIMESTAMPS tiledb_timestamps()
/**@}*/

/* ****************************** */
/*            VERSION             */
/* ****************************** */

/**
 *  Retrieves the version of the TileDB library currently being used.
 *
 *  @param major Will store the major version number.
 *  @param minor Will store the minor version number.
 *  @param rev Will store the revision (patch) number.
 */
TILEDB_EXPORT void tiledb_version(int32_t* major, int32_t* minor, int32_t* rev)
    TILEDB_NOEXCEPT;

/* ********************************* */
/*           TILEDB TYPES            */
/* ********************************* */

/** A TileDB query condition object. */
typedef struct tiledb_query_condition_t tiledb_query_condition_t;

/** A consolidation plan object. */
typedef struct tiledb_consolidation_plan_t tiledb_consolidation_plan_t;

/* ****************************** */
/*          QUERY CONDITION       */
/* ****************************** */

/**
 * Allocates a TileDB query condition object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_condition_t* query_condition;
 * tiledb_query_condition_alloc(ctx, &query_condition);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param cond The allocated query condition object.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_condition_alloc(
    tiledb_ctx_t* ctx, tiledb_query_condition_t** cond) TILEDB_NOEXCEPT;

/**
 * Frees a TileDB query condition object.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t value = 5;
 * tiledb_query_condition_t* query_condition;
 * tiledb_query_condition_alloc(
 *   ctx, "longitude", &value, sizeof(value), TILEDB_LT, &query_condition);
 * tiledb_query_set_condition(ctx, query, query_condition);
 * tiledb_query_submit(ctx, query);
 * tiledb_query_condition_free(&query_condition);
 * @endcode
 *
 * @param cond The query condition object to be freed.
 */
TILEDB_EXPORT void tiledb_query_condition_free(tiledb_query_condition_t** cond)
    TILEDB_NOEXCEPT;

/**
 * Initializes a TileDB query condition object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_condition_t* query_condition;
 * tiledb_query_condition_alloc(ctx, &query_condition);
 *
 * uint32_t value = 5;
 * tiledb_query_condition_init(
 *   ctx, query_condition, "longitude", &value, sizeof(value), TILEDB_LT);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param cond The allocated query condition object.
 * @param attribute_name The attribute name.
 * @param condition_value The value to compare against an attribute value.
 * @param condition_value_size The byte size of `condition_value`.
 * @param op The comparison operator.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_condition_init(
    tiledb_ctx_t* ctx,
    tiledb_query_condition_t* cond,
    const char* attribute_name,
    const void* condition_value,
    uint64_t condition_value_size,
    tiledb_query_condition_op_t op) TILEDB_NOEXCEPT;

/**
 * Combines two query condition objects into a newly allocated
 * condition. Does not mutate or free the input condition objects.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_condition_t* query_condition_1;
 * tiledb_query_condition_alloc(ctx, &query_condition_1);
 * uint32_t value_1 = 5;
 * tiledb_query_condition_init(
 *   ctx,
 *   query_condition_1,
 *   "longitude",
 *   &value_1,
 *   sizeof(value_1),
 *   TILEDB_LT);
 *
 * tiledb_query_condition_t* query_condition_2;
 * tiledb_query_condition_alloc(ctx, &query_condition_2);
 * uint32_t value_2 = 20;
 * tiledb_query_condition_init(
 *   ctx,
 *   query_condition_2,
 *   "latitude",
 *   &value_2,
 *   sizeof(value_2),
 *   TILEDB_GE);
 *
 * tiledb_query_condition_t* query_condition_3;
 * tiledb_query_condition_combine(
 *   ctx, query_condition_1, query_condition_2, TILEDB_AND, &query_condition_3);
 *
 * tiledb_query_condition_free(&query_condition_1);
 * tiledb_query_condition_free(&query_condition_2);
 *
 * tiledb_query_set_condition(ctx, query, query_condition_3);
 * tiledb_query_submit(ctx, query);
 * tiledb_query_condition_free(&query_condition_3);
 * @endcode
 *
 * @param[in]  ctx The TileDB context.
 * @param[in]  left_cond The first input condition.
 * @param[in]  right_cond The second input condition.
 * @param[in]  combination_op The combination operation.
 * @param[out] combined_cond The output condition holder.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_condition_combine(
    tiledb_ctx_t* ctx,
    const tiledb_query_condition_t* left_cond,
    const tiledb_query_condition_t* right_cond,
    tiledb_query_condition_combination_op_t combination_op,
    tiledb_query_condition_t** combined_cond) TILEDB_NOEXCEPT;

/**
 * Create a query condition representing a negation of the input query
 * condition. Currently this is performed by applying De Morgan's theorem
 * recursively to the query condition's internal representation.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_condition_t* query_condition_1;
 * tiledb_query_condition_alloc(ctx, &query_condition_1);
 * uint32_t value_1 = 5;
 * tiledb_query_condition_init(
 *   ctx,
 *   query_condition_1,
 *   "longitude",
 *   &value_1,
 *   sizeof(value_1),
 *   TILEDB_LT);
 *
 * tiledb_query_condition_t* query_condition_2;
 * tiledb_query_condition_negate(
 *   ctx,
 *   query_condition_1,
 *   &query_condition_2);
 *
 * tiledb_query_condition_free(&query_condition_1);
 *
 * tiledb_query_set_condition(ctx, query, query_condition_2);
 * tiledb_query_submit(ctx, query);
 * tiledb_query_condition_free(&query_condition_2);
 * @endcode
 *
 * @param[in]  ctx The TileDB context.
 * @param[in]  left_cond The first input condition.
 * @param[in]  right_cond The second input condition.
 * @param[in]  combination_op The combination operation.
 * @param[out] combined_cond The output condition holder.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_condition_negate(
    tiledb_ctx_t* ctx,
    const tiledb_query_condition_t* cond,
    tiledb_query_condition_t** negated_cond) TILEDB_NOEXCEPT;

/* ********************************* */
/*        ARRAY CONSOLIDATION        */
/* ********************************* */

/**
 * Depending on the consoliation mode in the config, consolidates either the
 * fragment files, fragment metadata files, or array metadata files into a
 * single file.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_consolidate(
 *     ctx, "s3:///tiledb_arrays/my_array", nullptr);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_uri The name of the TileDB array whose metadata will
 *     be consolidated.
 * @param[in] config Configuration parameters for the consolidation
 *     (`nullptr` means default, which will use the config from \p ctx).
 *     The `sm.consolidation.mode` parameter determines which type of
 *     consolidation to perform.
 *
 * @return `TILEDB_OK` on success, and `TILEDB_ERR` on error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_consolidate(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_config_t* config) TILEDB_NOEXCEPT;

/**
 * Cleans up the array, such as consolidated fragments and array metadata.
 * Note that this will coarsen the granularity of time traveling (see docs
 * for more information).
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_vacuum(ctx, "s3:///tiledb_arrays/my_array");
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_uri The name of the TileDB array to vacuum.
 * @param[in] config Configuration parameters for the vacuuming
 *     (`nullptr` means default, which will use the config from \p ctx).
 * @return `TILEDB_OK` on success, and `TILEDB_ERR` on error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_vacuum(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_config_t* config) TILEDB_NOEXCEPT;

/* ********************************* */
/*          OBJECT MANAGEMENT        */
/* ********************************* */

/**
 * Returns the TileDB object type for a given resource path.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_object_t type;
 * tiledb_object_type(ctx, "arrays/my_array", &type);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param path The URI path to the TileDB resource.
 * @param type The type to be retrieved.
 * @return `TILEDB_OK` on success, `TILEDB_ERR` on error.
 */
TILEDB_EXPORT int32_t tiledb_object_type(
    tiledb_ctx_t* ctx, const char* path, tiledb_object_t* type) TILEDB_NOEXCEPT;

/**
 * Deletes a TileDB resource (group, array, key-value).
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_object_remove(ctx, "arrays/my_array");
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param path The URI path to the tiledb resource.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_object_remove(tiledb_ctx_t* ctx, const char* path)
    TILEDB_NOEXCEPT;

/**
 * Moves a TileDB resource (group, array, key-value).
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_object_move(ctx, "arrays/my_array", "arrays/my_array_2");
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param old_path The old TileDB directory.
 * @param new_path The new TileDB directory.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_object_move(
    tiledb_ctx_t* ctx,
    const char* old_path,
    const char* new_path) TILEDB_NOEXCEPT;

/**
 * Walks (iterates) over the TileDB objects contained in *path*. The traversal
 * is done recursively in the order defined by the user. The user provides
 * a callback function which is applied on each of the visited TileDB objects.
 * The iteration continues for as long the callback returns non-zero, and stops
 * when the callback returns 0. Note that this function ignores any object
 * (e.g., file or directory) that is not TileDB-related.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_object_walk(ctx, "arrays", TILEDB_PREORDER, NULL, NULL);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param path The path in which the traversal will occur.
 * @param order The order of the recursive traversal (e.g., pre-order or
 *     post-order.
 * @param callback The callback function to be applied on every visited object.
 *     The callback should return `0` if the iteration must stop, and `1`
 *     if the iteration must continue. It takes as input the currently visited
 *     path, the type of that path (e.g., array or group), and the data
 *     provided by the user for the callback. The callback returns `-1` upon
 *     error. Note that `path` in the callback will be an **absolute** path.
 * @param data The data passed in the callback as the last argument.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_object_walk(
    tiledb_ctx_t* ctx,
    const char* path,
    tiledb_walk_order_t order,
    int32_t (*callback)(const char*, tiledb_object_t, void*),
    void* data) TILEDB_NOEXCEPT;

/**
 * Similar to `tiledb_walk`, but now the function visits only the children of
 * `path` (i.e., it does not recursively continue to the children directories).
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_object_ls(ctx, "arrays", NULL, NULL);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param path The path in which the traversal will occur.
 * @param callback The callback function to be applied on every visited object.
 *     The callback should return `0` if the iteration must stop, and `1`
 *     if the iteration must continue. It takes as input the currently visited
 *     path, the type of that path (e.g., array or group), and the data
 *     provided by the user for the callback. The callback returns `-1` upon
 *     error. Note that `path` in the callback will be an **absolute** path.
 * @param data The data passed in the callback as the last argument.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_object_ls(
    tiledb_ctx_t* ctx,
    const char* path,
    int32_t (*callback)(const char*, tiledb_object_t, void*),
    void* data) TILEDB_NOEXCEPT;

/* ****************************** */
/*              URI               */
/* ****************************** */

/**
 * Converts the given file URI to a null-terminated platform-native file path.
 *
 * **Example:**
 *
 * @code{.c}
 * char path[TILEDB_MAX_PATH];
 * uint32_t length = TILEDB_MAX_PATH; // Must be set to non-zero
 * tiledb_uri_to_path(ctx, "file:///my_array", path, &length);
 * // This will set "my_array" to `path`
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param uri The URI to be converted.
 * @param path_out The buffer where the converted path string is stored.
 * @param path_length The length of the path buffer. On return, this is set to
 * the length of the converted path string, or 0 on error.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note The path_out buffer must be allocated according to the platform's
 * maximum path length (e.g. `TILEDB_MAX_PATH), which includes space for the
 * terminating null character.
 */
TILEDB_EXPORT int32_t tiledb_uri_to_path(
    tiledb_ctx_t* ctx, const char* uri, char* path_out, uint32_t* path_length)
    TILEDB_NOEXCEPT;

/* ****************************** */
/*             Stats              */
/* ****************************** */

/**
 * Enable internal statistics gathering.
 *
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_stats_enable(void) TILEDB_NOEXCEPT;

/**
 * Disable internal statistics gathering.
 *
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_stats_disable(void) TILEDB_NOEXCEPT;

/**
 * Returns whether internal statistics gathering is enabled.
 *
 * @param enabled Output argument, non-zero for enabled and zero for disabled.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error
 */
TILEDB_EXPORT int32_t tiledb_stats_is_enabled(uint8_t* enabled) TILEDB_NOEXCEPT;

/**
 * Reset all internal statistics counters to 0.
 *
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_stats_reset(void) TILEDB_NOEXCEPT;

/**
 * Dump all internal statistics counters to some output (e.g.,
 * file or stdout).
 *
 * @param out The output.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_stats_dump(FILE* out) TILEDB_NOEXCEPT;

/**
 * Dump all internal statistics counters to an output string. The caller is
 * responsible for freeing the resulting string.
 *
 * **Example:**
 *
 * @code{.c}
 * char *stats_str;
 * tiledb_stats_dump_str(&stats_str);
 * // ...
 * tiledb_stats_free_str(&stats_str);
 * @endcode
 *
 * @param out Will be set to point to an allocated string containing the stats.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_stats_dump_str(char** out) TILEDB_NOEXCEPT;

/**
 * Dump all raw internal statistics counters to some output (e.g.,
 * file or stdout) as a JSON.
 *
 * @param out The output.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_stats_raw_dump(FILE* out) TILEDB_NOEXCEPT;

/**
 * Dump all raw internal statistics counters to a JSON-formatted output string.
 * The caller is responsible for freeing the resulting string.
 *
 * **Example:**
 *
 * @code{.c}
 * char *stats_str;
 * tiledb_stats_raw_dump_str(&stats_str);
 * // ...
 * tiledb_stats_raw_free_str(&stats_str);
 * @endcode
 *
 * @param out Will be set to point to an allocated string containing the stats.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_stats_raw_dump_str(char** out) TILEDB_NOEXCEPT;

/**
 *
 * Free the memory associated with a previously dumped stats string.
 *
 * @param out Pointer to a previously allocated stats string.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_stats_free_str(char** out) TILEDB_NOEXCEPT;

/* ****************************** */
/*          Heap Profiler         */
/* ****************************** */

/**
 * Enable heap profiling.
 *
 * @param file_name_prefix If empty or null, stats are dumped
 *   to stdout. If non-empty, this specifies the file_name prefix to
 *   write to. For example, value "tiledb_mem_stats" will write
 *   to "tiledb_mem_stats__1611170501", where the postfix is
 *   determined by the current epoch.
 * @param dump_interval_ms If non-zero, this spawns a dedicated
 *   thread to dump on this time interval.
 * @param dump_interval_bytes If non-zero, a dump will occur when
 *   the total number of lifetime allocated bytes is increased by
 *   more than this amount.
 * @param dump_threshold_bytes If non-zero, labeled allocations with
 *   a number of bytes lower than this threshold will not be reported
 *   in the dump.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_heap_profiler_enable(
    const char* file_name_prefix,
    uint64_t dump_interval_ms,
    uint64_t dump_interval_bytes,
    uint64_t dump_threshold_bytes) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

/* ********************************* */
/*           DEPRECATED API          */
/* ********************************* */
#ifndef TILEDB_REMOVE_DEPRECATIONS
#include "tiledb_deprecated.h"
#endif  // TILEDB_REMOVE_DEPRECATIONS

#endif  // TILEDB_H
