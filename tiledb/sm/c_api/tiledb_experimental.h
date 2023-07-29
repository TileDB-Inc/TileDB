/*
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

/*
 * API sections
 */
#include "tiledb/api/c_api/enumeration/enumeration_api_experimental.h"
#include "tiledb/api/c_api/group/group_api_external_experimental.h"
#include "tiledb/api/c_api/query_plan/query_plan_api_external_experimental.h"
#include "tiledb_dimension_label_experimental.h"

/* ********************************* */
/*               MACROS              */
/* ********************************* */

#ifdef __cplusplus
extern "C" {
#endif

/* ********************************* */
/*             LOGGING               */
/* ********************************* */

/**
 * Log a message at WARN level using TileDB's internal logging mechanism
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_log_warn(ctx, "This is a log message.");
 * @endcode
 *
 * @param ctx The TileDB Context.
 * @param message The message to log
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t
tiledb_log_warn(tiledb_ctx_t* ctx, const char* message);

/* ********************************* */
/*      ARRAY SCHEMA EVOLUTION       */
/* ********************************* */

/** A TileDB array schema. */
typedef struct tiledb_array_schema_evolution_t tiledb_array_schema_evolution_t;

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
    tiledb_array_schema_evolution_t** array_schema_evolution) TILEDB_NOEXCEPT;

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
    tiledb_array_schema_evolution_t** array_schema_evolution) TILEDB_NOEXCEPT;

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
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema_evolution The schema evolution.
 * @param[in] attribute The attribute to be added.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_attribute_t* attribute) TILEDB_NOEXCEPT;

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
    const char* attribute_name) TILEDB_NOEXCEPT;

/**
 * Adds an enumeration to an array schema evolution.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_enumeration_t* enmr;
 * void* data = get_data();
 * uint64_t data_size = get_data_size();
 * tiledb_enumeration_alloc(
 *     ctx,
 *     TILEDB_INT64,
 *     cell_val_num,
 *     FALSE,
 *     data,
 *     data_size,
 *     nullptr,
 *     0,
 *     &enumeration);
 * tiledb_array_schema_evolution_add_enumeration(ctx, array_schema_evolution,
 * enmr);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param enumeration The enumeration to be added.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_evolution_add_enumeration(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_enumeration_t* enumeration) TILEDB_NOEXCEPT;

/**
 * Drops an enumeration from an array schema evolution.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* enumeration_name = "enumeration_1";
 * tiledb_array_schema_evolution_drop_enumeration(ctx, array_schema_evolution,
 * enumeration_name);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param enumeration_name The name of the enumeration to be dropped.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_evolution_drop_enumeration(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    const char* enumeration_name) TILEDB_NOEXCEPT;

/**
 * Sets timestamp range in an array schema evolution
 * This function sets the output timestamp of the committed array schema after
 * evolution. The lo and hi values are currently required to be the same or else
 * an error is thrown.
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
 * @param lo The lower bound of timestamp range.
 * @param hi The upper bound of timestamp range, it must euqal to the lower
 * bound.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_set_timestamp_range(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    uint64_t lo,
    uint64_t hi) TILEDB_NOEXCEPT;

/* ********************************* */
/*          ARRAY SCHEMA             */
/* ********************************* */

/**
 * Gets timestamp range in an array schema evolution
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t timestamp_lo = 0;
 * uint64_t timestamp_hi = 0;
 * tiledb_array_schema_evolution_timestamp_range(ctx,
 * array_schema_evolution, &timestamp_lo, &timestamp_hi);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema object.
 * @param lo The lower bound of timestamp range.
 * @param hi The upper bound of timestamp range.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_timestamp_range(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    uint64_t* lo,
    uint64_t* hi) TILEDB_NOEXCEPT;

/**
 * Adds an enumeration to an array schema.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_enumeration_t* enumeration;
 * tiledb_enumeration_alloc(
 *     ctx,
 *     "enumeration_name",
 *     TILEDB_INT64,
 *     1,
 *     FALSE,
 *     data,
 *     data_size,
 *     nullptr,
 *     0,
 *     &enumeration);
 * tiledb_array_schema_add_enumeration(ctx, array_schema, enumeration);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param enumeration The enumeration to add with the attribute
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_add_enumeration(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_enumeration_t* enumeration) TILEDB_NOEXCEPT;

/**
 * Retrieves the schema of an array from the disk with all enumerations loaded,
 * creating an array schema struct.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_t* array_schema;
 * tiledb_array_schema_load(ctx, "s3://tiledb_bucket/my_array", &array_schema);
 * // Make sure to free the array schema in the end
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_uri The array whose schema will be retrieved.
 * @param array_schema The array schema to be retrieved, or `NULL` upon error.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_load_with_enumerations(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_t** array_schema) TILEDB_NOEXCEPT;

/* ********************************* */
/*      ATTRIBUTE ENUMERATIONS       */
/* ********************************* */

/**
 * Set the enumeration name on an attribute.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_attribute_set_enumeration_name(ctx, attr, "enumeration_name");
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The target attribute.
 * @param enumeration_name The name of the enumeration to use for the attribute.
 * @return `TILEDB_OK` for success, and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_attribute_set_enumeration_name(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    const char* enumeration_name) TILEDB_NOEXCEPT;

/**
 * Get the attribute's enumeration name if it has one.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_string_t* name;
 * tiledb_attribute_get_enumeration_name(ctx, attr, &name);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The target attribute.
 * @param name The name of the attribute, nullptr if the attribute does not
 *        have an associated enumeration.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_attribute_get_enumeration_name(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    tiledb_string_t** name) TILEDB_NOEXCEPT;

/* ********************************* */
/*               ARRAY               */
/* ********************************* */

/**
 * Deletes all written array data.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_delete(ctx, "hdfs:///temp/my_array");
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param uri The Array's URI.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_delete(tiledb_ctx_t* ctx, const char* uri)
    TILEDB_NOEXCEPT;

/**
 * Note: This API is deprecated and replaced with tiledb_array_delete (above).
 *
 * Deletes all written array data.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_delete_array(ctx, array, "hdfs:///temp/my_array");
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array The array to delete the data from.
 * @param uri The Array's URI.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_delete_array(
    tiledb_ctx_t* ctx, tiledb_array_t* array, const char* uri) TILEDB_NOEXCEPT;

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
    tiledb_array_schema_evolution_t* array_schema_evolution) TILEDB_NOEXCEPT;

/**
 * Retrieves an attribute's enumeration given the attribute name (key).
 *
 * **Example:**
 *
 * The following retrieves the first attribute in the schema.
 *
 * @code{.c}
 * tiledb_attribute_t* attr;
 * tiledb_array_schema_get_enumeration(
 *     ctx, array_schema, "attr_0", &enumeration);
 * // Make sure to delete the retrieved attribute in the end.
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array The TileDB array.
 * @param name The name (key) of the attribute from which to
 * retrieve the enumeration.
 * @param enumeration The enumeration object to retrieve.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_get_enumeration(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    const char* name,
    tiledb_enumeration_t** enumeration) TILEDB_NOEXCEPT;

/**
 * Load all enumerations for the array.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_load_all_enumerations(ctx, array);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array The TileDB array.
 * @param latest_only If non-zero, only load enumerations for the latest schema.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_load_all_enumerations(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    int latest_only) TILEDB_NOEXCEPT;

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
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_config_t* config) TILEDB_NOEXCEPT;

/* ********************************* */
/*               QUERY               */
/* ********************************* */

/**
 * Adds a query update values to be applied on an update.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t value = 5;
 * tiledb_query_add_update_value(
 *   ctx, query, "longitude", &value, sizeof(value), &update_value);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param field_name The attribute name.
 * @param update_value The value to set.
 * @param update_value_size The byte size of `update_value`.
 */
TILEDB_EXPORT int32_t tiledb_query_add_update_value(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* field_name,
    const void* update_value,
    uint64_t update_value_size) TILEDB_NOEXCEPT;

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
    uint64_t count) TILEDB_NOEXCEPT;

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
    uint64_t count) TILEDB_NOEXCEPT;

/**
 * Get the number of relevant fragments from the subarray. Should only be
 * called after size estimation was asked for.
 *
 * @param ctx The TileDB context.
 * @param query The query to get the data fron.
 * @param relevant_fragment_num Variable to receive the number of relevant
 * fragments.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note Should only be called after size estimation was run.
 */
TILEDB_EXPORT int32_t tiledb_query_get_relevant_fragment_num(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint64_t* relevant_fragment_num) TILEDB_NOEXCEPT;

/* ********************************* */
/*          QUERY CONDITION          */
/* ********************************* */

/**
 * Disable the use of enumerations on the given QueryCondition
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_condition_t* query_condition;
 * tiledb_query_condition_alloc(ctx, &query_condition);
 * uint32_t value = 5;
 * tiledb_query_condition_init(
 *   ctx,
 *   query_condition,
 *   "longitude",
 *   &value,
 *   sizeof(value),
 *   TILEDB_LT);
 * tiledb_query_condition_set_use_enumeration(ctx, query_condition, 0);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] cond The query condition
 * @param[in] use_enumeration Non-zero to use the associated enumeration
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_condition_set_use_enumeration(
    tiledb_ctx_t* ctx,
    const tiledb_query_condition_t* cond,
    int use_enumeration) TILEDB_NOEXCEPT;

/* ********************************* */
/*        QUERY STATUS DETAILS       */
/* ********************************* */

/** TileDB query status details type. */
typedef enum {
/** Helper macro for defining status details type enums. */
#define TILEDB_QUERY_STATUS_DETAILS_ENUM(id) TILEDB_##id
#include "tiledb_enum.h"
#undef TILEDB_QUERY_STATUS_DETAILS_ENUM
} tiledb_query_status_details_reason_t;

/** This should move to c_api/tiledb_struct_defs.h when stabilized */
typedef struct tiledb_experimental_query_status_details_t {
  tiledb_query_status_details_reason_t
      incomplete_reason;  ///< Reason enum for the incomplete query.
} tiledb_query_status_details_t;

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
    tiledb_query_status_details_t* status_details) TILEDB_NOEXCEPT;

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
TILEDB_EXPORT capi_return_t tiledb_ctx_alloc_with_error(
    tiledb_config_t* config,
    tiledb_ctx_t** ctx,
    tiledb_error_t** error) TILEDB_NOEXCEPT;

/* ********************************* */
/*           CONSOLIDATION           */
/* ********************************* */

/**
 * Consolidates the given fragment URIs into a single fragment.
 *
 * Note: This API needs to be used with caution until we implement
 * consolidation with timestamps. For now, if the non-empty domain of the
 * consolidated fragments overlap anything in the fragments that come in
 * between, this could lead to unpredictable behavior.
 *
 * **Example:**
 *
 * @code{.c}

 * const char* uris[2]={"__0_0_0807b1428b6c4ff48b3cdb3283ca7903_10",
 *                      "__1_1_d9d965753d224194965575c1e9cdeeda_10"};
 * tiledb_array_consolidate_fragments(ctx, "my_array", uris, 2, nullptr);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_uri The name of the TileDB array whose metadata will
 *     be consolidated.
 * @param[in] fragment_uris URIs of the fragments to consolidate.
 * @param[in] num_fragments Number of URIs to consolidate.
 * @param config Configuration parameters for the consolidation
 *     (`nullptr` means default, which will use the config from \p ctx).
 *
 * @return `TILEDB_OK` on success, and `TILEDB_ERR` on error.
 */
TILEDB_EXPORT int32_t tiledb_array_consolidate_fragments(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const char** fragment_uris,
    const uint64_t num_fragments,
    tiledb_config_t* config) TILEDB_NOEXCEPT;

/* ********************************* */
/*                FILESTORE          */
/* ********************************* */

/**
 * Creates an array schema based on the properties of the provided URI
 * or a default schema if no URI is provided
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_t* schema;
 * tiledb_filestore_schema_create(ctx, "/path/file.pdf", &schema);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param uri The file URI.
 * @param array_schema The TileDB array schema to be created
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_filestore_schema_create(
    tiledb_ctx_t* ctx,
    const char* uri,
    tiledb_array_schema_t** array_schema) TILEDB_NOEXCEPT;

/**
 * Imports a file into a TileDB filestore array
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_t* schema;
 * tiledb_filestore_schema_create(ctx, path_to_file, &schema);
 * tiledb_array_create(ctx, path_to_array, schema);
 * tiledb_filestore_uri_import(ctx, path_to_array, path_to_file,
 * TILEDB_MIME_AUTODETECT);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param filestore_array_uri The array URI.
 * @param file_uri The file URI.
 * @param mime_type The mime type of the file
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_filestore_uri_import(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    const char* file_uri,
    tiledb_mime_type_t mime_type) TILEDB_NOEXCEPT;

/**
 * Exports a filestore array into a bare file
 * **Example:**
 *
 * @code{.c}
 * tiledb_filestore_uri_export(ctx, path_to_file, path_to_array);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file_uri The file URI.
 * @param filestore_array_uri The array URI.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_filestore_uri_export(
    tiledb_ctx_t* ctx,
    const char* file_uri,
    const char* filestore_array_uri) TILEDB_NOEXCEPT;

/**
 * Writes size bytes starting at address buf into filestore array
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_t* schema;
 * tiledb_filestore_schema_create(ctx, NULL, &schema);
 * tiledb_array_create(ctx, path_to_array, schema);
 * tiledb_filestore_buffer_import(ctx, path_to_array, buf, size,
 * TILEDB_MIME_AUTODETECT);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param filestore_array_uri The array URI.
 * @param buf The input buffer
 * @param size Number of bytes to be imported
 * @param mime_type The mime type of the data
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_filestore_buffer_import(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    void* buf,
    size_t size,
    tiledb_mime_type_t mime_type) TILEDB_NOEXCEPT;

/**
 * Dump the content of a filestore array into a buffer
 * **Example:**
 *
 * @code{.c}
 * size_t size = 1024;
 * void *buf = malloc(size);
 * tiledb_filestore_buffer_export(ctx, path_to_array, 0, buf, size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param filestore_array_uri The array URI.
 * @param offset The offset at which we should start exporting from the array
 * @param buf The buffer that will contain the filestore array content
 * @param size The number of bytes to be exported into the buffer
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_filestore_buffer_export(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    size_t offset,
    void* buf,
    size_t size) TILEDB_NOEXCEPT;

/**
 * Get the uncompressed size of a filestore array
 * **Example:**
 *
 * @code{.c}
 * size_t size;
 * tiledb_filestore_size(ctx, path_to_array, &size);
 * void *buf = malloc(size);
 * tiledb_filestore_buffer_export(ctx, path_to_array, 0, buf, size);
 * free(buf);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] filestore_array_uri The array URI.
 * @param[in] size The returned uncompressed size of the filestore array
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_filestore_size(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    size_t* size) TILEDB_NOEXCEPT;

/**
 * Get the string representation of a mime type enum
 *
 * @param mime_type The mime enum
 * @param str The resulted string representation
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_mime_type_to_str(
    tiledb_mime_type_t mime_type, const char** str) TILEDB_NOEXCEPT;

/**
 * Turn a string mime type into a TileDB enum
 *
 * @param str The mime type string
 * @param mime_type The resulted mime enum
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_mime_type_from_str(
    const char* str, tiledb_mime_type_t* mime_type) TILEDB_NOEXCEPT;

/**
 * Retrieves the number of cells written to the fragments by the user.
 *
 * Contributions from each fragment to the total are as described in following.
 *
 * In the case of sparse fragments, this is the number of non-empty
 * cells in the fragment.
 *
 * In the case of dense fragments, TileDB may add fill
 * values to populate partially populated tiles. Those fill values
 * are counted in the returned number of cells. In other words,
 * the cell number is derived from the number of *integral* tiles
 * written in the file.
 *
 * note: The count returned is the cumulative total of cells
 * written to all fragments in the current fragment_info entity,
 * i.e. count may effectively include multiples for any cells that
 * may be overlapping across the various fragments.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t cell_num;
 * tiledb_fragment_info_get_total_cell_num(ctx, fragment_info, &cell_num);
 * @endcode
 *
 * @param[in]  ctx The TileDB context
 * @param[in]  fragment_info The fragment info object.
 * @param[out] count The number of cells to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_fragment_info_get_total_cell_num(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint64_t* count) TILEDB_NOEXCEPT;

/**
 * Creates a consolidation plan object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_consolidation_plan_t* consolidation_plan;
 * tiledb_consolidation_plan_create_with_mbr(ctx, array, 1024*1024*1024,
 * &consolidation_plan);
 * @endcode
 *
 * @param ctx TileDB context.
 * @param array The array to create the plan for.
 * @param fragment_size The desired fragment size.
 * @param consolidation_plan The consolidation plan object to be created and
 * populated.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_consolidation_plan_create_with_mbr(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t fragment_size,
    tiledb_consolidation_plan_t** consolidation_plan) TILEDB_NOEXCEPT;

/**
 * Frees a consolidation plan object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_consolidation_plan_free(&consolidation_plan);
 * @endcode
 *
 * @param consolidation_plan The consolidation plan object to be freed.
 */
TILEDB_EXPORT void tiledb_consolidation_plan_free(
    tiledb_consolidation_plan_t** consolidation_plan) TILEDB_NOEXCEPT;

/**
 * Get the number of nodes of a consolidation plan object.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t num_nodes;
 * tiledb_consolidation_plan_get_num_nodes(ctx, consolidation_plan, &num_nodes);
 * @endcode
 *
 * @param ctx TileDB context.
 * @param consolidation_plan The consolidation plan.
 * @param num_nodes The number of nodes to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_consolidation_plan_get_num_nodes(
    tiledb_ctx_t* ctx,
    tiledb_consolidation_plan_t* consolidation_plan,
    uint64_t* num_nodes) TILEDB_NOEXCEPT;

/**
 * Get the number of fragments for a specific node of a consolidation plan
 * object.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t num_fragments;
 * tiledb_consolidation_plan_get_num_fragments(ctx, consolidation_plan, 0,
 * &num_fragments);
 * @endcode
 *
 * @param ctx TileDB context.
 * @param consolidation_plan The consolidation plan.
 * @param node_index The node index.
 * @param num_fragments The number of fragments to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_consolidation_plan_get_num_fragments(
    tiledb_ctx_t* ctx,
    tiledb_consolidation_plan_t* consolidation_plan,
    uint64_t node_index,
    uint64_t* num_fragments) TILEDB_NOEXCEPT;

/**
 * Get the number of fragments for a specific node of a consolidation plan
 * object.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t num_fragments;
 * tiledb_consolidation_plan_get_num_fragments(ctx, consolidation_plan, 0,
 * &num_fragments);
 * @endcode
 *
 * @param ctx TileDB context.
 * @param consolidation_plan The consolidation plan.
 * @param node_index The node index.
 * @param fragment_index The fragment index.
 * @param uri The fragment uri to be retreived.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_consolidation_plan_get_fragment_uri(
    tiledb_ctx_t* ctx,
    tiledb_consolidation_plan_t* consolidation_plan,
    uint64_t node_index,
    uint64_t fragment_index,
    const char** uri) TILEDB_NOEXCEPT;

/**
 * Dumps the consolidation plan in JSON format to a null terminated string. The
 * string needs to be freed with tiledb_consolidation_plan_free_json_str.
 *
 * **Example:**
 *
 * @code{.c}
 * char* str;
 * tiledb_consolidation_plan_dump_json_str(ctx, consolidation_plan, str);
 * tiledb_consolidation_plan_free_json_str(str);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param consolidation_plan The consolidation plan.
 * @param str The output string.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_consolidation_plan_dump_json_str(
    tiledb_ctx_t* ctx,
    const tiledb_consolidation_plan_t* consolidation_plan,
    char** str) TILEDB_NOEXCEPT;

/**
 * Frees a string created by tiledb_consolidation_plan_dump_json_str.
 *
 * **Example:**
 *
 * @code{.c}
 * char* str;
 * tiledb_consolidation_plan_dump_json_str(ctx, consolidation_plan, str);
 * tiledb_consolidation_plan_free_json_str(str);
 * @endcode
 *
 * @param str The string to be freed.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_consolidation_plan_free_json_str(char** str)
    TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_EXPERIMENTAL_H
