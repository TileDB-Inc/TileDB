/**
 * @file tiledb/api/c_api/query/query_api_external_experimental.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2026 TileDB, Inc.
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
 * This file declares experimental C API for TileDB queries.
 */

#ifndef TILEDB_CAPI_QUERY_API_EXTERNAL_EXPERIMENTAL_H
#define TILEDB_CAPI_QUERY_API_EXTERNAL_EXPERIMENTAL_H

#include "query_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

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

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_QUERY_API_EXTERNAL_EXPERIMENTAL_H
