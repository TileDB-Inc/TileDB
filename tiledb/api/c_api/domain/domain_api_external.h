/**
 * @file tiledb/api/c_api/domain/domain_api_external.h
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
 */

#ifndef TILEDB_CAPI_DOMAIN_EXTERNAL_H
#define TILEDB_CAPI_DOMAIN_EXTERNAL_H

#include "../context/context_api_external.h"
#include "../datatype/datatype_api_external.h"
#include "../dimension/dimension_api_external.h"
#include "../string/string_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/** A TileDB domain. */
typedef struct tiledb_domain_handle_t tiledb_domain_t;

/**
 * Creates a TileDB domain.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_domain_t* domain;
 * tiledb_domain_alloc(ctx, &domain);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param domain The TileDB domain to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_domain_alloc(
    tiledb_ctx_t* ctx, tiledb_domain_t** domain) TILEDB_NOEXCEPT;

/**
 * Destroys a TileDB domain, freeing associated memory.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_domain_t* domain;
 * tiledb_domain_alloc(ctx, &domain);
 * tiledb_domain_free(&domain);
 * @endcode
 *
 * @param domain The domain to be destroyed.
 */
TILEDB_EXPORT void tiledb_domain_free(tiledb_domain_t** domain) TILEDB_NOEXCEPT;

/**
 * Retrieves the domain's type.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_datatype_t type;
 * tiledb_domain_get_type(ctx, domain, &type);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param domain The domain.
 * @param type The type to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_domain_get_type(
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    tiledb_datatype_t* type) TILEDB_NOEXCEPT;

/**
 * Retrieves the number of dimensions in a domain.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t dim_num;
 * tiledb_domain_get_ndim(ctx, domain, &dim_num);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param domain The domain
 * @param ndim The number of dimensions in a domain.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_domain_get_ndim(
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    uint32_t* ndim) TILEDB_NOEXCEPT;

/**
 * Adds a dimension to a TileDB domain.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_dimension_t* dim;
 * int64_t dim_domain[] = {1, 10};
 * int64_t tile_extent = 5;
 * tiledb_dimension_alloc(
 *     ctx, "dim_0", TILEDB_INT64, dim_domain, &tile_extent, &dim);
 * tiledb_domain_add_dimension(ctx, domain, dim);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param domain The domain to add the dimension to.
 * @param dim The dimension to be added.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_domain_add_dimension(
    tiledb_ctx_t* ctx,
    tiledb_domain_t* domain,
    tiledb_dimension_t* dim) TILEDB_NOEXCEPT;

/**
 * Retrieves a dimension object from a domain by index.
 *
 * **Example:**
 *
 * The following retrieves the first dimension from a domain.
 *
 * @code{.c}
 * tiledb_dimension_t* dim;
 * tiledb_domain_get_dimension_from_index(ctx, domain, 0, &dim);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param domain The domain to add the dimension to.
 * @param index The index of domain dimension
 * @param dim The retrieved dimension object.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_domain_get_dimension_from_index(
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    uint32_t index,
    tiledb_dimension_t** dim) TILEDB_NOEXCEPT;

/**
 * Retrieves a dimension object from a domain by name (key).
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_dimension_t* dim;
 * tiledb_domain_get_dimension_from_name(ctx, domain, "dim_0", &dim);
 * @endcode
 *
 * @param ctx The TileDB context
 * @param domain The domain to add the dimension to.
 * @param name The name (key) of the requested dimension
 * @param dim The retrieved dimension object.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_domain_get_dimension_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    const char* name,
    tiledb_dimension_t** dim) TILEDB_NOEXCEPT;

/**
 * Checks whether the domain has a dimension of the given name.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t has_dim;
 * tiledb_domain_has_dimension(ctx, domain, "dim_0", &has_dim);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param domain The domain.
 * @param name The name of the dimension to check for.
 * @param has_dim Set to `1` if the domain has a dimension of the given name,
 *      else `0`.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_domain_has_dimension(
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    const char* name,
    int32_t* has_dim) TILEDB_NOEXCEPT;

#ifndef TILEDB_REMOVE_DEPRECATIONS
/**
 * Dumps the info of a domain in ASCII form to some output (e.g.,
 * file or `stdout`).
 *
 * **Example:**
 *
 * The following prints the domain dump to the standard output.
 *
 * @code{.c}
 * tiledb_domain_dump(ctx, domain, stdout);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param domain The domain.
 * @param out The output.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_domain_dump(
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    FILE* out) TILEDB_NOEXCEPT;
#endif

/**
 * Dumps the contents of a domain in ASCII form to the selected string output.
 *
 * The output string handle must be freed by the user after use.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_string_t* tdb_string;
 * tiledb_domain_dump_str(ctx, domain, &tdb_string);
 * // Use the string
 * tiledb_string_free(&tdb_string);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param domain The domain.
 * @param out The output string.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_domain_dump_str(
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    tiledb_string_t** out) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif
