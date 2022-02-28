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

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_EXPERIMENTAL_H
