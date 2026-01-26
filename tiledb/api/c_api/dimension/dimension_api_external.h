/**
 * @file tiledb/api/c_api/dimension/dimension_api_external.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB, Inc.
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
 * This file declares the dimension section of the C API for TileDB.
 */

#ifndef TILEDB_CAPI_DIMENSION_EXTERNAL_H
#define TILEDB_CAPI_DIMENSION_EXTERNAL_H

#include "../api_external_common.h"
#include "../datatype/datatype_api_external.h"
#include "../filter_list/filter_list_api_external.h"
#include "../string/string_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/** A TileDB dimension. */
typedef struct tiledb_dimension_handle_t tiledb_dimension_t;

/**
 * Creates a dimension.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_dimension_t* dim;
 * int64_t dim_domain[] = {1, 10};
 * int64_t tile_extent = 5;
 * tiledb_dimension_alloc(
 *     ctx, "dim_0", TILEDB_INT64, dim_domain, &tile_extent, &dim);
 * @endcode
 *
 * Note: as laid out in the Storage Format,
 * the following Datatypes are not valid for Dimension:
 * TILEDB_CHAR, TILEDB_BLOB, TILEDB_GEOM_WKB, TILEDB_GEOM_WKT, TILEDB_BOOL,
 * TILEDB_STRING_UTF8, TILEDB_STRING_UTF16, TILEDB_STRING_UTF32,
 * TILEDB_STRING_UCS2, TILEDB_STRING_UCS4, TILEDB_ANY
 *
 * @param ctx The TileDB context.
 * @param name The dimension name.
 * @param type The dimension type.
 * @param dim_domain The dimension domain.
 * @param tile_extent The dimension tile extent.
 * @param dim The dimension to be created.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_dimension_alloc(
    tiledb_ctx_t* ctx,
    const char* name,
    tiledb_datatype_t type,
    const void* dim_domain,
    const void* tile_extent,
    tiledb_dimension_t** dim) TILEDB_NOEXCEPT;

/**
 * Destroys a TileDB dimension, freeing associated memory.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_dimension_free(&dim);
 * @endcode
 *
 * @param dim The dimension to be destroyed.
 */
TILEDB_EXPORT void tiledb_dimension_free(tiledb_dimension_t** dim)
    TILEDB_NOEXCEPT;

/**
 * Sets the filter list for a dimension.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_list_t* filter_list;
 * tiledb_filter_list_alloc(ctx, &filter_list);
 * tiledb_filter_list_add_filter(ctx, filter_list, filter);
 * tiledb_dimension_set_filter_list(ctx, dim, filter_list);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dim The target dimension.
 * @param filter_list The filter_list to be set.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_dimension_set_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_dimension_t* dim,
    tiledb_filter_list_t* filter_list) TILEDB_NOEXCEPT;

/**
 * Sets the number of values per cell for a dimension. If this is not
 * used, the default is `1`.
 *
 * **Examples:**
 *
 * For a fixed-sized dimension:
 *
 * @code{.c}
 * tiledb_dimension_set_cell_val_num(ctx, dim, 3);
 * @endcode
 *
 * For a variable-sized dimension:
 *
 * @code{.c}
 * tiledb_dimension_set_cell_val_num(ctx, dim, TILEDB_VAR_NUM);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dim The target dimension.
 * @param cell_val_num The number of values per cell.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_dimension_set_cell_val_num(
    tiledb_ctx_t* ctx,
    tiledb_dimension_t* dim,
    uint32_t cell_val_num) TILEDB_NOEXCEPT;

/**
 * Retrieves the filter list for a dimension.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_list_t* filter_list;
 * tiledb_dimension_get_filter_list(ctx, dim, &filter_list);
 * tiledb_filter_list_free(&filter_list);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dim The target dimension.
 * @param filter_list The filter list to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_dimension_get_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_dimension_t* dim,
    tiledb_filter_list_t** filter_list) TILEDB_NOEXCEPT;

/**
 * Retrieves the number of values per cell for a dimension. For variable-sized
 * dimensions the result is TILEDB_VAR_NUM.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t num;
 * tiledb_dimension_get_cell_val_num(ctx, dim, &num);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dim The dimension.
 * @param cell_val_num The number of values per cell to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_dimension_get_cell_val_num(
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dim,
    uint32_t* cell_val_num) TILEDB_NOEXCEPT;

/**
 * Retrieves the dimension name.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* dim_name;
 * tiledb_dimension_get_name(ctx, dim, &dim_name);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dim The dimension.
 * @param name The name to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_dimension_get_name(
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dim,
    const char** name) TILEDB_NOEXCEPT;

/**
 * Retrieves the dimension type.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_datatype_t dim_type;
 * tiledb_dimension_get_type(ctx, dim, &dim_type);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dim The dimension.
 * @param type The type to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_dimension_get_type(
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dim,
    tiledb_datatype_t* type) TILEDB_NOEXCEPT;

/**
 * Retrieves the domain of the dimension.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t* domain;
 * tiledb_dimension_get_domain(ctx, dim, &domain);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dim The dimension.
 * @param domain The domain to be retrieved. Note that the defined type of
 *     input `domain` must be the same as the dimension type, otherwise the
 *     behavior is unpredictable (it will probably segfault).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_dimension_get_domain(
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dim,
    const void** domain) TILEDB_NOEXCEPT;

/**
 * Retrieves the tile extent of the dimension.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t* tile_extent;
 * tiledb_dimension_get_tile_extent(ctx, dim, &tile_extent);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dim The dimension.
 * @param tile_extent The tile extent (pointer) to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_dimension_get_tile_extent(
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dim,
    const void** tile_extent) TILEDB_NOEXCEPT;

/**
 * Dumps the contents of a dimension in ASCII form to the selected string
 * output.
 *
 * The output string handle must be freed by the user after use.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_string_t* tdb_string;
 * tiledb_dimension_dump_str(ctx, dimension, &tdb_string);
 * // Use the string
 * tiledb_string_free(&tdb_string);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param dimension The dimension.
 * @param out The output string.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_dimension_dump_str(
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dimension,
    tiledb_string_t** out) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_DIMENSION_EXTERNAL_H
