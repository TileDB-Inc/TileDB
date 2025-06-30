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

#include "../string/string_api_external.h"
#include "tiledb/api/c_api/api_external_common.h"
#include "tiledb/api/c_api/context/context_api_external.h"
#include "tiledb/api/c_api/domain/domain_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/** C API struct to specify the limits of a dimension for an ND rectangle. */
typedef struct {
  const void* min;
  uint64_t min_size;
  const void* max;
  uint64_t max_size;
} tiledb_range_t;

typedef struct tiledb_ndrectangle_handle_t tiledb_ndrectangle_t;

/**
 * Allocate an N-dimensional rectangle given a TileDB array schema domain.
 * The resulted rectangle will maintain the same number of dimensions as the
 * array schema domain.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_ndrectangle_t *ndr;
 * tiledb_ndrectangle_alloc(ctx, domain, &ndr);
 * tiledb_ndrectangle_free(&ndr);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param domain The TileDB array schema domain
 * @param ndr The n-dimensional rectangle to be allocated
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_ndrectangle_alloc(
    tiledb_ctx_t* ctx,
    tiledb_domain_t* domain,
    tiledb_ndrectangle_t** ndr) TILEDB_NOEXCEPT;

/**
 * Free the resources associated with the N-dimensional rectangle arg
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_ndrectangle_t *ndr;
 * tiledb_ndrectangle_alloc(ctx, domain, &ndr);
 * tiledb_ndrectangle_free(&ndr);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param ndr The n-dimensional rectangle to be freed
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_ndrectangle_free(tiledb_ndrectangle_t** ndr)
    TILEDB_NOEXCEPT;

/**
 * Get the range set on an N-dimensional rectangle for a dimension name
 *
 * The pointers within the returned range struct point to resources tied to
 * the lifetime of the `tiledb_ndrectangle_t` object, it is not the
 * responsibility of the caller to free those resources, attempting
 * to do so results in undefined behavior.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_range_t range;
 * tiledb_ndrectangle_get_range_from_name(ctx, ndr, "dim", &range);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param ndr The n-dimensional rectangle to be queried
 * @param name The name of the dimension for which range should be returned
 * @param range The range returned as output argument
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_ndrectangle_get_range_from_name(
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    const char* name,
    tiledb_range_t* range) TILEDB_NOEXCEPT;

/**
 * Get the range set on an N-dimensional rectangle for a dimension index.
 *
 * The pointers within the returned range struct point to resources tied to
 * the lifetime of the `tiledb_ndrectangle_t` object, it is not the
 * responsibility of the caller to free those resources, attempting
 * to do so results in undefined behavior.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_range_t range;
 * tiledb_ndrectangle_get_range_from_name(ctx, ndr, 1, &range);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param ndr The n-dimensional rectangle to be queried
 * @param idx The index of the dimension for which range should be returned
 * @param range The range returned as output argument
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_ndrectangle_get_range(
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    uint32_t idx,
    tiledb_range_t* range) TILEDB_NOEXCEPT;

/**
 * Set the range on an N-dimensional rectangle for a dimension name
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_range_t range;
 * range.min = &min;
 * range.min_size = sizeof(min);
 * range.max = &max;
 * range.max_size = sizeof(max);
 * tiledb_ndrectangle_set_range_for_name(ctx, ndr, "dim", &range);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param ndr The n-dimensional rectangle to be queried
 * @param name The name of the dimension for which range should be set
 * @param range The range to be set on the ND rectangle
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_ndrectangle_set_range_for_name(
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    const char* name,
    tiledb_range_t* range) TILEDB_NOEXCEPT;

/**
 * Set the range on an N-dimensional rectangle for dimension at idx.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_range_t range;
 * range.min = &min;
 * range.min_size = sizeof(min);
 * range.max = &max;
 * range.max_size = sizeof(max);
 * tiledb_ndrectangle_set_range(ctx, ndr, 1, &range);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param ndr The n-dimensional rectangle to be queried
 * @param idx The index of the dimension for which range should be set
 * @param range The range to be set on the ND rectangle
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_ndrectangle_set_range(
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    uint32_t idx,
    tiledb_range_t* range) TILEDB_NOEXCEPT;

/**
 * Get the TileDB datatype for dimension at idx from
 * the N-dimensional rectangle passed as argument
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_datatype_t type;
 * tiledb_ndrectangle_get_dtype(ctx, ndr, 1, &type);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param ndr The n-dimensional rectangle to be queried
 * @param idx The index of the dimension
 * @param type The datatype to be returned
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_ndrectangle_get_dtype(
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    uint32_t idx,
    tiledb_datatype_t* type) TILEDB_NOEXCEPT;

/**
 * Get the TileDB datatype for dimension name from
 * the N-dimensional rectangle passed as argument
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_datatype_t type;
 * tiledb_ndrectangle_get_dtype_from_name(ctx, ndr, "dim1", &type);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param ndr The n-dimensional rectangle to be queried
 * @param name The dimension name
 * @param type The datatype to be returned
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_ndrectangle_get_dtype_from_name(
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    const char* name,
    tiledb_datatype_t* type) TILEDB_NOEXCEPT;

/**
 * Get the the number of dimensions of
 * the N-dimensional rectangle passed as argument
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t ndim;
 * tiledb_ndrectangle_get_dim_num(ctx, ndr, &ndim);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param ndr The n-dimensional rectangle to be queried
 * @param ndim The number of dimensions to be returned
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_ndrectangle_get_dim_num(
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    uint32_t* ndim) TILEDB_NOEXCEPT;

/**
 * Dumps the contents of an ndrectangle in ASCII form to the selected string
 * output.
 *
 * The output string handle must be freed by the user after use.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_string_t* tdb_string;
 * tiledb_ndrectangle_dump_str(ctx, ndr, &tdb_string);
 * // Use the string
 * tiledb_string_free(&tdb_string);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param ndr The ndrectangle.
 * @param out The output string.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_ndrectangle_dump_str(
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    tiledb_string_t** out) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_NDRECTANGLE_API_EXTERNAL_EXPERIMENTAL_H
