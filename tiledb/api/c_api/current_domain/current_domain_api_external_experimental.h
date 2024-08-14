/**
 * @file
 * tiledb/api/c_api/current_domain/current_domain_api_external_experimental.h
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
 * This file declares the current_domain experimental C API for TileDB.
 */

#ifndef TILEDB_CAPI_CURRENT_DOMAIN_API_EXTERNAL_EXPERIMENTAL_H
#define TILEDB_CAPI_CURRENT_DOMAIN_API_EXTERNAL_EXPERIMENTAL_H

#include "tiledb/api/c_api/api_external_common.h"
#include "tiledb/api/c_api/context/context_api_external.h"
#include "tiledb/api/c_api/ndrectangle/ndrectangle_api_external_experimental.h"

#ifdef __cplusplus
extern "C" {
#endif

/** The current domain type */
typedef enum {
#define TILEDB_CURRENT_DOMAIN_TYPE_ENUM(id) TILEDB_##id
#include "tiledb/api/c_api/current_domain/current_domain_api_enum.h"
#undef TILEDB_CURRENT_DOMAIN_TYPE_ENUM
} tiledb_current_domain_type_t;

typedef struct tiledb_current_domain_handle_t tiledb_current_domain_t;

/**
 * Create a current domain object
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_current_domain_t *current_domain;
 * tiledb_current_domain_create(ctx, &current_domain);
 * tiledb_current_domain_free(&current_domain);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param current_domain The current domain to be allocated
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_current_domain_create(
    tiledb_ctx_t* ctx,
    tiledb_current_domain_t** current_domain) TILEDB_NOEXCEPT;

/**
 * Free the resources associated with a current domain object
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_current_domain_t *current_domain;
 * tiledb_current_domain_create(ctx, &current_domain);
 * tiledb_current_domain_free(&current_domain);
 * @endcode
 *
 * @param current_domain The current domain to be freed
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_current_domain_free(
    tiledb_current_domain_t** current_domain) TILEDB_NOEXCEPT;

/**
 * Set a N-dimensional rectangle representation on a current domain.
 * Error if the current domain passed is not empty.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_current_domain_t *current_domain;
 * tiledb_current_domain_create(ctx, &current_domain);
 * tiledb_ndrectangle_t *ndr;
 * tiledb_ndrectangle_alloc(ctx, domain, &ndr);
 *
 * tiledb_range_t range;
 * range.min = &min;
 * range.min_size = sizeof(min);
 * range.max = &max;
 * range.max_size = sizeof(max);
 * tiledb_ndrectangle_set_range_for_name(ctx, ndr, "dim", &range);
 *
 * tiledb_current_domain_set_ndrectangle(ctx, current_domain, ndr);
 *
 * tiledb_ndrectangle_free(&ndr);
 * tiledb_current_domain_free(&current_domain);
 * @endcode
 *
 * @param ctx A TileDB context
 * @param current_domain The current domain to modify
 * @param ndr The N-dimensional rectangle to be set
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_current_domain_set_ndrectangle(
    tiledb_ctx_t* ctx,
    tiledb_current_domain_t* current_domain,
    tiledb_ndrectangle_t* ndr) TILEDB_NOEXCEPT;

/**
 * Get the N-dimensional rectangle associated with the current domain object.
 * Error if the current domain is empty or a different representation is set.
 *
 * It is the responsability of the caller to free the resources associated
 * with the ndrectangle when the handle isn't needed anymore.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_ndrectangle_t *ndr;
 * tiledb_current_domain_get_ndrectangle(ctx, current_domain, &ndr);
 * @endcode
 *
 * @param ctx A TileDB context
 * @param current_domain The current domain to query
 * @param ndr The N-dimensional rectangle of the current domain
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_current_domain_get_ndrectangle(
    tiledb_ctx_t* ctx,
    tiledb_current_domain_t* current_domain,
    tiledb_ndrectangle_t** ndr) TILEDB_NOEXCEPT;

/**
 * Query if the current domain object is empty or not.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t empty = 0;
 * tiledb_current_domain_get_is_empty(ctx, current_domain, &empty);
 * @endcode
 *
 * @param ctx A TileDB context
 * @param current_domain The current domain to query
 * @param is_empty True if nothing is set on the current domain
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_current_domain_get_is_empty(
    tiledb_ctx_t* ctx,
    tiledb_current_domain_t* current_domain,
    uint32_t* is_empty) TILEDB_NOEXCEPT;

/**
 * Query the type of current domain set on the object
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_current_domain_type_t type;
 * tiledb_current_domain_get_type(ctx, current_domain, &type);
 * @endcode
 *
 * @param ctx A TileDB context
 * @param current_domain The current domain to query
 * @param type The type of representation set on the current domain
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_current_domain_get_type(
    tiledb_ctx_t* ctx,
    tiledb_current_domain_t* current_domain,
    tiledb_current_domain_type_t* type) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_CURRENT_DOMAIN_API_EXTERNAL_EXPERIMENTAL_H
