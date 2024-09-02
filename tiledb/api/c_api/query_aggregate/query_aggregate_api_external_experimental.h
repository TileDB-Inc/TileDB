/**
 * @file
 * tiledb/api/c_api/query_aggregate/query_aggregate_api_external_experimental.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file declares the query_aggregate experimental C API for TileDB.
 */

#ifndef TILEDB_CAPI_QUERY_AGGREGATE_API_EXTERNAL_EXPERIMENTAL_H
#define TILEDB_CAPI_QUERY_AGGREGATE_API_EXTERNAL_EXPERIMENTAL_H

#include "../api_external_common.h"
#include "../string/string_api_external.h"
#include "tiledb/api/c_api/context/context_api_external.h"
#include "tiledb/api/c_api/query/query_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tiledb_query_channel_handle_t tiledb_query_channel_t;

typedef struct tiledb_channel_operation_handle_t tiledb_channel_operation_t;

typedef struct tiledb_channel_operator_handle_t tiledb_channel_operator_t;

// Constant aggregate operator handles
TILEDB_EXPORT extern const tiledb_channel_operator_t*
    tiledb_channel_operator_sum;

TILEDB_EXPORT extern const tiledb_channel_operator_t*
    tiledb_channel_operator_min;

TILEDB_EXPORT extern const tiledb_channel_operator_t*
    tiledb_channel_operator_max;

TILEDB_EXPORT extern const tiledb_channel_operator_t*
    tiledb_channel_operator_mean;

TILEDB_EXPORT extern const tiledb_channel_operator_t*
    tiledb_channel_operator_null_count;

// Constant aggregate operation handles
TILEDB_EXPORT extern const tiledb_channel_operation_t* tiledb_aggregate_count;

/**
 * Helper function to access the constant SUM channel operator handle
 * **Example:**
 *
 * @code{.c}
 * const tiledb_channel_operator_t *operator_sum;
 * tiledb_channel_operator_sum_get(ctx, &operator_sum);
 * tiledb_channel_operation_t* sum_A;
 * tiledb_create_unary_aggregate(ctx, query, operator_sum, "A", sum_A);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param op The operator handle to be retrieved
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_channel_operator_sum_get(
    tiledb_ctx_t* ctx, const tiledb_channel_operator_t** op) TILEDB_NOEXCEPT;

/**
 * Helper function to access the constant MIN channel operator handle
 * **Example:**
 *
 * @code{.c}
 * const tiledb_channel_operator_t *operator_min;
 * tiledb_channel_operator_min_get(ctx, &operator_min);
 * tiledb_channel_operation_t* min_A;
 * tiledb_create_unary_aggregate(ctx, query, operator_min, "A", min_A);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param op The operator handle to be retrieved
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_channel_operator_min_get(
    tiledb_ctx_t* ctx, const tiledb_channel_operator_t** op) TILEDB_NOEXCEPT;

/**
 * Helper function to access the constant MAX channel operator handle
 * **Example:**
 *
 * @code{.c}
 * const tiledb_channel_operator_t *operator_max;
 * tiledb_channel_operator_max_get(ctx, &operator_max);
 * tiledb_channel_operation_t* max_A;
 * tiledb_create_unary_aggregate(ctx, query, operator_max, "A", max_A);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param op The operator handle to be retrieved
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_channel_operator_max_get(
    tiledb_ctx_t* ctx, const tiledb_channel_operator_t** op) TILEDB_NOEXCEPT;

/**
 * Helper function to acces the constant COUNT aggregate operation handle
 * **Example:**
 *
 * @code{.c}
 * const tiledb_channel_operation_t* count_aggregate;
 * tiledb_aggregate_count_get(ctx, &count_aggregate);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param operation The operation handle to be retrieved
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_aggregate_count_get(
    tiledb_ctx_t* ctx,
    const tiledb_channel_operation_t** operation) TILEDB_NOEXCEPT;

/**
 * Helper function to access the constant MEAN channel operator handle
 * **Example:**
 *
 * @code{.c}
 * const tiledb_channel_operator_t *operator_mean;
 * tiledb_channel_operator_mean_get(ctx, &operator_mean);
 * tiledb_channel_operation_t* mean_A;
 * tiledb_create_unary_aggregate(ctx, query, operator_mean, "A", mean_A);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param op The operator handle to be retrieved
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_channel_operator_mean_get(
    tiledb_ctx_t* ctx, const tiledb_channel_operator_t** op) TILEDB_NOEXCEPT;

/**
 * Helper function to access the constant NULL_COUNT channel operator handle
 * **Example:**
 *
 * @code{.c}
 * const tiledb_channel_operator_t *operator_nullcount;
 * tiledb_channel_operator_null_count_get(ctx, &operator_nullcount);
 * tiledb_channel_operation_t* nullcount_A;
 * tiledb_create_unary_aggregate(ctx, query, operator_nullcount, "A",
 * nullcount_A);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param op The operator handle to be retrieved
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_channel_operator_null_count_get(
    tiledb_ctx_t* ctx, const tiledb_channel_operator_t** op) TILEDB_NOEXCEPT;

/**
 * Gets the default channel of the query. The default channel consists of all
 * the rows the query would return as if executed standalone.
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_channel_t* default_channel;
 * tiledb_query_get_default_channel(ctx, query, &default_channel);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query A TileDB query
 * @param channel The channel handle to be allocated
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_default_channel(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_query_channel_t** channel) TILEDB_NOEXCEPT;

/**
 * Create a channel operation given an input field and an operator.
 * **Example:**
 *
 * @code{.c}
 * tiledb_channel_operation_t* sum_A;
 * tiledb_create_unary_aggregate(ctx, query, tiledb_aggregate_sum, "A",
 * &sum_A);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query A TileDB query
 * @param op The operator to be applied on an attribute
 * @param input_field_name The attribute on which the operator will be applied
 * @param operation The operation handle to be allocated
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_create_unary_aggregate(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const tiledb_channel_operator_t* op,
    const char* input_field_name,
    tiledb_channel_operation_t** operation) TILEDB_NOEXCEPT;

/**
 * Add an aggregate operation on a query channel.
 * The result computed by the aggregate operation will be available
 * via the `output_field_name` buffer.
 * **Example:**
 *
 * @code{.c}
 * tiledb_channel_apply_aggregate(ctx, default_channel, "sumA", sum_A);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query A TileDB query
 * @param channel The query channel on which to apply the operation
 * @param output_field_name The attribute to be created that holds the result
 * @param operation The operation to be applied
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_channel_apply_aggregate(
    tiledb_ctx_t* ctx,
    tiledb_query_channel_t* channel,
    const char* output_field_name,
    const tiledb_channel_operation_t* operation) TILEDB_NOEXCEPT;

/**
 * Frees the resources associated with a TileDB channel operation object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_channel_operation *(..., &op);
 * tiledb_aggregate_free(ctx, &op);
 * @endcode
 *
 * @param ctx A TileDB context
 * @param op A TileDB channel operation handle
 */
TILEDB_EXPORT capi_return_t tiledb_aggregate_free(
    tiledb_ctx_t* ctx, tiledb_channel_operation_t** op) TILEDB_NOEXCEPT;

/**
 * Frees the resources associated with a TileDB query channel object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_channel_t* default_channel;
 * tiledb_query_get_default_channel(ctx, query, &default_channel);
 * tiledb_query_channel_free(ctx, &default_channel);
 * @endcode
 *
 * @param ctx A TileDB context
 * @param channel A TileDB query channel handle
 */
TILEDB_EXPORT capi_return_t tiledb_query_channel_free(
    tiledb_ctx_t* ctx, tiledb_query_channel_t** channel) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_QUERY_AGGREGATE_API_EXTERNAL_EXPERIMENTAL_H
