/**
 * @file tiledb/api/c_api/query_condition/query_condition_api_external.h
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
 * This file declares the query condition C API for TileDB.
 */

#ifndef TILEDB_CAPI_QUERY_CONDITION_API_EXTERNAL_H
#define TILEDB_CAPI_QUERY_CONDITION_API_EXTERNAL_H

#include "../api_external_common.h"
#include "../context/context_api_external.h"

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
#include "tiledb/sm/c_api/tiledb_enum.h"
#undef TILEDB_QUERY_CONDITION_COMBINATION_OP_ENUM
} tiledb_query_condition_combination_op_t;

/* ********************************* */
/*           TILEDB TYPES            */
/* ********************************* */

/** A TileDB query condition object. */
typedef struct tiledb_query_condition_t tiledb_query_condition_t;

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

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_QUERY_CONDITION_API_EXTERNAL_H
