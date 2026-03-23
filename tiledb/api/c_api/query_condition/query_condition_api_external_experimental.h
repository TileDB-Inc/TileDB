/**
 * @file tiledb/api/c_api/query_condition/query_condition_api_external_experimental.h
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
 * This file declares experimental query condition C API for TileDB.
 */

#ifndef TILEDB_CAPI_QUERY_CONDITION_API_EXPERIMENTAL_H
#define TILEDB_CAPI_QUERY_CONDITION_API_EXPERIMENTAL_H

#include "query_condition_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

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

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_QUERY_CONDITION_API_EXPERIMENTAL_H
