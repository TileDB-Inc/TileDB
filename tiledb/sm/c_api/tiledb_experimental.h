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
#include "tiledb/api/c_api/current_domain/current_domain_api_external_experimental.h"
#include "tiledb/api/c_api/enumeration/enumeration_api_experimental.h"
#include "tiledb/api/c_api/fragment_info/fragment_info_api_experimental.h"
#include "tiledb/api/c_api/profile/profile_api_experimental.h"
#include "tiledb/api/c_api/query/query_api_external_experimental.h"
#include "tiledb/api/c_api/query_aggregate/query_aggregate_api_external_experimental.h"
#include "tiledb/api/c_api/query_condition/query_condition_api_external_experimental.h"
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
