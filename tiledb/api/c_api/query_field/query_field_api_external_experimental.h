/**
 * @file
 * tiledb/api/c_api/query_field/query_field_api_external_experimental.h
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
 * This file declares the query field C API for TileDB.
 */

#ifndef TILEDB_CAPI_QUERY_FIELD_API_EXTERNAL_EXPERIMENTAL_H
#define TILEDB_CAPI_QUERY_FIELD_API_EXTERNAL_EXPERIMENTAL_H

#include "../api_external_common.h"
#include "tiledb/api/c_api/datatype/datatype_api_external.h"
#include "tiledb/api/c_api/query_aggregate/query_aggregate_api_external_experimental.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tiledb_query_field_handle_t tiledb_query_field_t;

/** TileDB query field origin. */
typedef enum {
  TILEDB_ATTRIBUTE_FIELD = 0,
  TILEDB_DIMENSION_FIELD,
  TILEDB_AGGREGATE_FIELD
} tiledb_field_origin_t;

/**
 * Get a query field handle for the field name passed as argument.
 * It is the responsability of the caller to manage the lifetime of the
 * field handle and `tiledb_query_field_free` it when appropriate.
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_field_t *field;
 * tiledb_query_get_field(ctx, query, "dimX", &field);
 * tiledb_query_field_free(ctx, &field);
 *
 * @endcode
 *
 * @param ctx The TileDB context
 * @param query A TileDB query
 * @param field_name The name of the field for which a handle should be created
 * @param field The query field handle
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_query_get_field(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* field_name,
    tiledb_query_field_t** field) TILEDB_NOEXCEPT;

/**
 * Frees the resources associated with a TileDB query field handle
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_field_t *field;
 * tiledb_query_get_field(ctx, query, "dimX", &field);
 * tiledb_query_field_free(ctx, &field);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param field The address of the query field handle pointer
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_query_field_free(
    tiledb_ctx_t* ctx, tiledb_query_field_t** field) TILEDB_NOEXCEPT;

/**
 * Get the TileDB datatype for a query field
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_field_t *field;
 * tiledb_query_get_field(ctx, query, "dimX", &field);
 * tiledb_datatype_t t;
 * tiledb_field_datatype(ctx, field, &t);
 * tiledb_query_field_free(ctx, &field);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param field The query field handle
 * @param type The TileDB datatype to be returned for the field
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_field_datatype(
    tiledb_ctx_t* ctx,
    tiledb_query_field_t* field,
    tiledb_datatype_t* type) TILEDB_NOEXCEPT;

/**
 * Get the number of values per cell for a field
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_field_t *field;
 * tiledb_query_get_field(ctx, query, "dimX", &field);
 * uint32_t cell_val_num = 0;
 * tiledb_field_cell_val_num(ctx, field, &cell_val_num);
 * tiledb_query_field_free(ctx, &field);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param field The query field handle
 * @param cell_val_num The number of values per cell to be returned
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_field_cell_val_num(
    tiledb_ctx_t* ctx,
    tiledb_query_field_t* field,
    uint32_t* cell_val_num) TILEDB_NOEXCEPT;

/**
 * Retrieves the nullability of a field.
 *
 * **Example:**
 *
 * @code{.c}
 * uint8_t nullable;
 * tiledb_field_get_nullable(ctx, attr, &nullable);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param field The query field handle
 * @param nullable Output argument, non-zero for nullable and zero
 *    for non-nullable.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_field_get_nullable(
    tiledb_ctx_t* ctx,
    tiledb_query_field_t* field,
    uint8_t* nullable) TILEDB_NOEXCEPT;

/**
 * Get the origin type of the passed field
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_field_t *field;
 * tiledb_query_get_field(ctx, query, "dimX", &field);
 * tiledb_field_origin_t origin;
 * tiledb_field_origin(ctx, field, &origin);
 * check_true(origin == TILEDB_DIMENSION_FIELD);
 * tiledb_query_field_free(ctx, &field);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param field The query field handle
 * @param origin The origin type of the field to be returned
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_field_origin(
    tiledb_ctx_t* ctx,
    tiledb_query_field_t* field,
    tiledb_field_origin_t* origin) TILEDB_NOEXCEPT;

/**
 * Get the query channel a field it's on
 * At the moment, all fields are on the query default channel.
 * Aggregates segmentation will add the ability for multiple channels
 * to be created and this API will enable quering which channel based
 * on the field.
 * This API allocates a new query channel handle when called, it's the
 * responsability of the caller to free the new query channel handle.
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_field_t *field;
 * tiledb_query_get_field(ctx, query, "SumX", &field);
 * tiledb_query_channel_t *channel;
 * tiledb_field_channel(ctx, field, &channel);
 * tiledb_query_channel_free(ctx, &channel);
 * tiledb_query_field_free(ctx, &field);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param field The query field handle
 * @param channel The allocated handle for the query channel the field it's on
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_field_channel(
    tiledb_ctx_t* ctx,
    tiledb_query_field_t* field,
    tiledb_query_channel_t** channel) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_QUERY_FIELD_API_EXTERNAL_EXPERIMENTAL_H
