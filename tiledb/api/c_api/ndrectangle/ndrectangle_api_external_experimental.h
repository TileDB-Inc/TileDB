/**
 * @file
 * tiledb/api/c_api/ndrectangle/ndrectangle_api_external_experimental.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file declares the NDRectangle experimental C API for TileDB.
 */

#ifndef TILEDB_CAPI_NDRECTANGLE_API_EXTERNAL_EXPERIMENTAL_H
#define TILEDB_CAPI_NDRECTANGLE_API_EXTERNAL_EXPERIMENTAL_H

#include "../api_external_common.h"
#include "tiledb/api/c_api/context/context_api_external.h"
#include "tiledb/api/c_api/domain/domain_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * C API struct exposed to the user to help specify
 * the limits of a dimension for a ND rectangle.
 */
typedef struct {
  void* min;
  uint64_t min_size;
  void* max;
  uint64_t max_size;
} tiledb_range_t;

/**
 * C API carrier TODO
 *
 */
typedef struct tiledb_ndrectangle_handle_t tiledb_ndrectangle_t;

/**
 * TODO
 * **Example:**
 *
 * @code{.c}
 * TODO
 * @endcode
 *
 * @param ctx The TileDB context
 * @param domain The TileDB array schema domain
 * @param ndr The n-dimensional rectangle to be allocated
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_ndrectangle_alloc(
    tiledb_ctx_t* ctx,
    tiledb_domain_t* domain,
    tiledb_ndrectangle_t** ndr) TILEDB_NOEXCEPT;

/**
 * TODO
 * **Example:**
 *
 * @code{.c}
 * TODO
 * @endcode
 *
 * @param ndr The n-dimensional rectangle to be freed
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_ EXPORT int32_t tiledb_ndrectangle_free(tiledb_ndrectangle_t** ndr)
    TILEDB_NOEXCEPT;

/**
 * TODO
 * **Example:**
 *
 * @code{.c}
 * TODO
 * @endcode
 *
 * @param ctx The TileDB context
 * @param ndr The n-dimensional rectangle to be queried
 * @param name The name of the dimension for which range should be returned
 * @param range The range returned as output argument
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_ndrectangle_get_range_from_name(
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    const char* name,
    tiledb_range_t* range) TILEDB_NOEXCEPT;

/**
 * TODO
 * **Example:**
 *
 * @code{.c}
 * TODO
 * @endcode
 *
 * @param ctx The TileDB context
 * @param ndr The n-dimensional rectangle to be queried
 * @param idx The index of the dimension for which range should be returned
 * @param range The range returned as output argument
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_ndrectangle_get_range(
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    uint32_t idx,
    tiledb_range_t* range) TILEDB_NOEXCEPT;

/**
 * TODO
 * **Example:**
 *
 * @code{.c}
 * TODO
 * @endcode
 *
 * @param ctx The TileDB context
 * @param ndr The n-dimensional rectangle to be queried
 * @param name The name of the dimension for which range should be set
 * @param range The range to be set on the ND rectangle
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_ndrectangle_set_range_for_name(
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    const char* name,
    tiledb_range_t* range) TILEDB_NOEXCEPT;

/**
 * TODO
 * **Example:**
 *
 * @code{.c}
 * TODO
 * @endcode
 *
 * @param ctx The TileDB context
 * @param ndr The n-dimensional rectangle to be queried
 * @param idx The index of the dimension for which range should be set
 * @param range The range to be set on the ND rectangle
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_ndrectangle_set_range(
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    uint32_t idx,
    tiledb_range_t* range) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_NDRECTANGLE_API_EXTERNAL_EXPERIMENTAL_H
