/**
 * @file tiledb/api/c_api/filter/filter_api_experimental.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file declares the filter section of the experimental TileDB C API.
 */

#ifndef TILEDB_CAPI_FILTER_EXPERIMENTAL_H
#define TILEDB_CAPI_FILTER_EXPERIMENTAL_H

#include "../api_external_common.h"
#include "../datatype/datatype_api_external.h"
#include "filter_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Returns the datatype of the filter option.
 *
 * @param[in] option The filter optional to get the type of.
 * @param[out] type The data type of the filter option.
 */
TILEDB_EXPORT capi_return_t tiledb_filter_option_datatype(
    tiledb_filter_option_t option, tiledb_datatype_t* type);

/**
 * Returns if an option has a non-null value.
 *
 * @param[in] ctx The TileDB context.
 * @param[in] filter The TileDB filter.
 * @param[in] option Filter option to check for.
 * @param[out] has_option Set to `1` the filter has a value set for the
 * requested option exists, else `0`
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filter_has_option(
    tiledb_ctx_t* ctx,
    tiledb_filter_t* filter,
    tiledb_filter_option_t option,
    int32_t* has_option);

/**
 * Returns the number of valid options on the filter.
 *
 * @param[in] ctx The TileDB context.
 * @param[in] filter The TileDB filter.
 * @param[out] valid_option_num The number of options that this filter has.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filter_get_valid_option_num(
    tiledb_ctx_t* ctx, tiledb_filter_t* filter, uint32_t* valid_option_num);

/**
 * Returns the option type by index.
 *
 * @param[in] ctx The TileDB context.
 * @param[in] filter The TileDB filter.
 * @param[in] index The index of the option to retrieve the type of.
 * @param[out] option The option type to retreive.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_filter_valid_option_from_index(
    tiledb_ctx_t* ctx,
    tiledb_filter_t* filter,
    int32_t index,
    tiledb_filter_option_t* option);

#ifdef __cplusplus
}
#endif

#endif
