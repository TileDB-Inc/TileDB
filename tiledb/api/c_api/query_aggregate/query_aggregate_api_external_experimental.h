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
 * This file declares the query_aggregate C API for TileDB.
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

/**
 * Create a SUM channel operator.
 * **Example:**
 *
 * @code{.c}
 * tiledb_channel_operator_t *op_sum;
 * tiledb_channel_create_operator_sum(ctx, &op_sum);
 * tiledb_channel_operation_t* sum_A;
 * tiledb_channel_create_operation_field(ctx, query, op_sum, "A", &sum_A);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param operator The operator handle to be allocated
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_channel_create_operator_sum(
    tiledb_ctx_t* ctx, tiledb_channel_operator_t** op) TILEDB_NOEXCEPT;

/**
 * Create a COUNT channel operator.
 * **Example:**
 *
 * @code{.c}
 * tiledb_channel_operator_t *op_count;
 * tiledb_channel_create_operator_count(ctx, &op_count);
 * tiledb_channel_operation_t* count_A;
 * tiledb_channel_create_operation_field(ctx, query, op_count, "A", &count_A);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param operator The operator handle to be allocated
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_channel_create_operator_count(
    tiledb_ctx_t* ctx, tiledb_channel_operator_t** op) TILEDB_NOEXCEPT;

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
 * tiledb_channel_create_operation_field(ctx, query, operator, "A",
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
TILEDB_EXPORT int32_t tiledb_channel_create_operation_field(
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
 * tiledb_channel_add_aggregate(ctx, default_channel, "sumA", sum_A);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query A TileDB query
 * @param channel The query channel on which to apply the operation
 * @param output_field_name The attribute to be created that holds the result
 * @param operation The operation to be applied
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_channel_add_aggregate(
    tiledb_ctx_t* ctx,
    tiledb_query_channel_t* channel,
    const char* output_field_name,
    tiledb_channel_operation_t* operation) TILEDB_NOEXCEPT;

/**
 * Frees the resources associated with a TileDB channel operation object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_channel_operation *op;
 * tiledb_channel_create_operation_field(..., &op);
 * tiledb_channel_operation_free(ctx, &op);
 * @endcode
 *
 * @param ctx A TileDB context
 * @param op A TileDB channel operation handle
 */
TILEDB_EXPORT capi_return_t tiledb_channel_operation_free(
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

/**
 * Frees the resources associated with a TileDB channel operator object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_channel_operator_t *op_sum;
 * tiledb_channel_create_operator_sum(ctx, query, &op_sum);
 * tiledb_channel_operator_free(&op_sum);
 * @endcode
 *
 * @param ctx A TileDB context
 * @param op A TileDB channel operator handle
 */
TILEDB_EXPORT capi_return_t tiledb_channel_operator_free(
    tiledb_ctx_t* ctx, tiledb_channel_operator_t** op) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_QUERY_AGGREGATE_API_EXTERNAL_EXPERIMENTAL_H
