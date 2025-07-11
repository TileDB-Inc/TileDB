/*
 * @file   tiledb_experimental.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2025 TileDB, Inc.
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
#include "tiledb/api/c_api/array/array_api_experimental.h"
#include "tiledb/api/c_api/array_schema/array_schema_api_experimental.h"
#include "tiledb/api/c_api/array_schema_evolution/array_schema_evolution_api_experimental.h"
#include "tiledb/api/c_api/attribute/attribute_api_external_experimental.h"
#include "tiledb/api/c_api/context/context_api_experimental.h"
#include "tiledb/api/c_api/current_domain/current_domain_api_external_experimental.h"
#include "tiledb/api/c_api/enumeration/enumeration_api_experimental.h"
#include "tiledb/api/c_api/fragment_info/fragment_info_api_experimental.h"
#include "tiledb/api/c_api/profile/profile_api_experimental.h"
#include "tiledb/api/c_api/query_aggregate/query_aggregate_api_external_experimental.h"
#include "tiledb/api/c_api/query_field/query_field_api_external_experimental.h"
#include "tiledb/api/c_api/query_plan/query_plan_api_external_experimental.h"
#include "tiledb/api/c_api/subarray/subarray_api_experimental.h"
#include "tiledb/api/c_api/vfs/vfs_api_experimental.h"
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
tiledb_log_warn(tiledb_ctx_t* ctx, const char* message) TILEDB_NOEXCEPT;

/* ********************************* */
/*              AS BUILT             */
/* ********************************* */

/**
 * Dumps the TileDB build configuration to a string.
 *
 * **Example**
 * @code{.c}
 * tiledb_string_t* out;
 * tiledb_as_built_dump(&out);
 * tiledb_string_free(&out);
 * @endcode
 *
 * @param out The output.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_as_built_dump(tiledb_string_t** out)
    TILEDB_NOEXCEPT;

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
 * Extends an enumeration during array schema evolution.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_enumeration_t* original_enmr = get_existing_enumeration();
 * const void* data = get_new_data();
 * uint64_t data_size = get_new_data_size();
 * tiledb_enumeration_t* new_enmr;
 * tiledb_enumeration_extend(
 *     ctx,
 *     original_enmr,
 *     data,
 *     data_size,
 *     nullptr,
 *     0,
 *     &new_enmr);
 * tiledb_array_schema_evolution_extend_enumeration(
 *     ctx,
 *     array_schema_evolution,
 *     new_enmr);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param enumeration The enumeration to be extended. This should be the result
 *        of a call to tiledb_enumeration_extend.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_evolution_extend_enumeration(
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

/**
 * Expands the current domain during array schema evolution.
 * TileDB will enforce that the new current domain is expanding
 * on the current one and not contracting during `tiledb_array_evolve`.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_current_domain_t *new_domain;
 * tiledb_current_domain_create(ctx, &new_domain);
 * tiledb_ndrectangle_t *ndr;
 * tiledb_ndrectangle_alloc(ctx, domain, &ndr);
 *
 * tiledb_range_t range;
 * range.min = &expanded_min;
 * range.min_size = sizeof(expanded_min);
 * range.max = &expanded_max;
 * range.max_size = sizeof(expanded_max);
 * tiledb_ndrectangle_set_range_for_name(ctx, ndr, "dim1", &range);
 * tiledb_ndrectangle_set_range_for_name(ctx, ndr, "dim2", &range);
 *
 * tiledb_current_domain_set_ndrectangle(new_domain, ndr);
 *
 * tiledb_array_schema_evolution_expand_current_domain(ctx,
 *      array_schema_evolution, new_domain);
 *
 * tiledb_array_evolve(ctx, array_uri, array_schema_evolution);
 *
 * tiledb_ndrectangle_free(&ndr);
 * tiledb_current_domain_free(&new_domain);
 *
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param expanded_domain The current domain we want to expand the schema to.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_evolution_expand_current_domain(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_current_domain_t* expanded_domain) TILEDB_NOEXCEPT;

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
 * Initializes a TileDB query condition set membership object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_condition_t* cond
 * tiledb_query_condition_alloc_set_membership(
 *   ctx,
 *   "some_name",
 *   data,
 *   data_size,
 *   offsets,
 *   offsets_size,
 *   TILEDB_QUERY_CONDITION_OP_IN,
 *   &cond);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param field_name The field name.
 * @param data A pointer to the set member data.
 * @param data_size The length of the data buffer.
 * @param offsets A pointer to the array of offsets of members.
 * @param offsets_size The length of the offsets array in bytes.
 * @param op The set membership operator to use.
 * @param cond The allocated query condition object.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_query_condition_alloc_set_membership(
    tiledb_ctx_t* ctx,
    const char* field_name,
    const void* data,
    uint64_t data_size,
    const void* offsets,
    uint64_t offests_size,
    tiledb_query_condition_op_t op,
    tiledb_query_condition_t** cond) TILEDB_NOEXCEPT;

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
/*           QUERY PREDICATE         */
/* ********************************* */

/**
 * Adds a predicate to be applied to a read query. The added predicate
 * will be analyzed and evaluated in the subarray step, query condition
 * step, or both.
 *
 * The predicate is parsed as an Apache DataFusion SQL expression and must
 * evaluate to a boolean.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* pred = "(row BETWEEN 1 AND 10) OR (column BETWEEN 1 AND 10)";
 * tiledb_query_add_predicate(ctx, query, pred);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param predicate A text representation of the desired predicate.
 */
TILEDB_EXPORT capi_return_t tiledb_query_add_predicate(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* predicate) TILEDB_NOEXCEPT;

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

/* ********************************* */
/*         ARRAY CONSOLIDATION       */
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
 * @param[in] fragment_uris Fragment names of the fragments to consolidate. The
 *     names can be recovered using tiledb_fragment_info_get_fragment_name_v2.
 * @param[in] num_fragments Number of URIs to consolidate.
 * @param[in] config Configuration parameters for the consolidation
 *     (`nullptr` means default, which will use the config from \p ctx).
 *
 * @return `TILEDB_OK` on success, and `TILEDB_ERR` on error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_consolidate_fragments(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const char** fragment_uris,
    const uint64_t num_fragments,
    tiledb_config_t* config) TILEDB_NOEXCEPT;

/* ********************************* */
/*         CONSOLIDATION PLAN        */
/* ********************************* */

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
