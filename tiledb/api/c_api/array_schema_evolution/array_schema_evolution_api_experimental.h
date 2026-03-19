/**
 * @file
 * tiledb/api/c_api/array_schema_evolution/array_schema_evolution_api_experimental.h
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
 * This file declares the array schema evolution section of the experimental
 * TileDB C API.
 */

#ifndef TILEDB_CAPI_ARRAY_SCHEMA_EVOLUTION_EXPERIMENTAL_H
#define TILEDB_CAPI_ARRAY_SCHEMA_EVOLUTION_EXPERIMENTAL_H

#include "../api_external_common.h"
#include "../attribute/attribute_api_external.h"
#include "../context/context_api_external.h"
#include "../current_domain/current_domain_api_external_experimental.h"
#include "../enumeration/enumeration_api_experimental.h"

#ifdef __cplusplus
extern "C" {
#endif

/** C API carrier for a TileDB array schema evolution */
typedef struct tiledb_array_schema_evolution_t tiledb_array_schema_evolution_t;

/**
 * Creates a TileDB schema evolution object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_evolution_t* array_schema_evolution;
 * tiledb_array_schema_evolution_alloc(ctx, &array_schema_evolution);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The TileDB schema evolution to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_alloc(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t** array_schema_evolution) TILEDB_NOEXCEPT;

/**
 * Destroys an array schema evolution, freeing associated memory.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_evolution_free(&array_schema_evolution);
 * @endcode
 *
 * @param array_schema_evolution The array schema evolution to be destroyed.
 */
TILEDB_EXPORT void tiledb_array_schema_evolution_free(
    tiledb_array_schema_evolution_t** array_schema_evolution) TILEDB_NOEXCEPT;

/**
 * Adds an attribute to an array schema evolution.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_attribute_t* attr;
 * tiledb_attribute_alloc(ctx, "my_attr", TILEDB_INT32, &attr);
 * tiledb_array_schema_evolution_add_attribute(ctx, array_schema_evolution,
 * attr);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_schema_evolution The schema evolution.
 * @param[in] attribute The attribute to be added.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_attribute_t* attribute) TILEDB_NOEXCEPT;

/**
 * Drops an attribute to an array schema evolution.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* attribute_name="a1";
 * tiledb_array_schema_evolution_drop_attribute(ctx, array_schema_evolution,
 * attribute_name);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param attribute_name The name of the attribute to be dropped.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_drop_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    const char* attribute_name) TILEDB_NOEXCEPT;

/**
 * Adds an enumeration to an array schema evolution.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_enumeration_t* enmr;
 * void* data = get_data();
 * uint64_t data_size = get_data_size();
 * tiledb_enumeration_alloc(
 *     ctx,
 *     TILEDB_INT64,
 *     cell_val_num,
 *     FALSE,
 *     data,
 *     data_size,
 *     nullptr,
 *     0,
 *     &enumeration);
 * tiledb_array_schema_evolution_add_enumeration(ctx, array_schema_evolution,
 * enmr);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param enumeration The enumeration to be added.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_evolution_add_enumeration(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_enumeration_t* enumeration) TILEDB_NOEXCEPT;

/**
 * Extends an enumeration during array schema evolution.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_enumeration_t* original_enmr = get_existing_enumeration();
 * const void* data = get_new_data();
 * uint64_t data_size = get_new_data_size();
 * tiledb_enumeration_t* new_enmr;
 * tiledb_enumeration_extend(
 *     ctx,
 *     original_enmr,
 *     data,
 *     data_size,
 *     nullptr,
 *     0,
 *     &new_enmr);
 * tiledb_array_schema_evolution_extend_enumeration(
 *     ctx,
 *     array_schema_evolution,
 *     new_enmr);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param enumeration The enumeration to be extended. This should be the result
 *        of a call to tiledb_enumeration_extend.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_evolution_extend_enumeration(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_enumeration_t* enumeration) TILEDB_NOEXCEPT;

/**
 * Drops an enumeration from an array schema evolution.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* enumeration_name = "enumeration_1";
 * tiledb_array_schema_evolution_drop_enumeration(ctx, array_schema_evolution,
 * enumeration_name);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param enumeration_name The name of the enumeration to be dropped.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_evolution_drop_enumeration(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    const char* enumeration_name) TILEDB_NOEXCEPT;

/**
 * Sets timestamp range in an array schema evolution
 * This function sets the output timestamp of the committed array schema after
 * evolution. The lo and hi values are currently required to be the same or else
 * an error is thrown.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t timestamp = tiledb_timestamp_now_ms();
 * tiledb_array_schema_evolution_set_timestamp_range(ctx,
 * array_schema_evolution, timestamp, timestamp);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param lo The lower bound of timestamp range.
 * @param hi The upper bound of timestamp range, it must euqal to the lower
 * bound.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_set_timestamp_range(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    uint64_t lo,
    uint64_t hi) TILEDB_NOEXCEPT;

/**
 * Expands the current domain during array schema evolution.
 * TileDB will enforce that the new current domain is expanding
 * on the current one and not contracting during `tiledb_array_evolve`.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_current_domain_t *new_domain;
 * tiledb_current_domain_create(ctx, &new_domain);
 * tiledb_ndrectangle_t *ndr;
 * tiledb_ndrectangle_alloc(ctx, domain, &ndr);
 *
 * tiledb_range_t range;
 * range.min = &expanded_min;
 * range.min_size = sizeof(expanded_min);
 * range.max = &expanded_max;
 * range.max_size = sizeof(expanded_max);
 * tiledb_ndrectangle_set_range_for_name(ctx, ndr, "dim1", &range);
 * tiledb_ndrectangle_set_range_for_name(ctx, ndr, "dim2", &range);
 *
 * tiledb_current_domain_set_ndrectangle(new_domain, ndr);
 *
 * tiledb_array_schema_evolution_expand_current_domain(ctx,
 *      array_schema_evolution, new_domain);
 *
 * tiledb_array_evolve(ctx, array_uri, array_schema_evolution);
 *
 * tiledb_ndrectangle_free(&ndr);
 * tiledb_current_domain_free(&new_domain);
 *
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param expanded_domain The current domain we want to expand the schema to.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_array_schema_evolution_expand_current_domain(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_current_domain_t* expanded_domain) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_ARRAY_SCHEMA_EVOLUTION_EXPERIMENTAL_H
