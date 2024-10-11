/**
 * @file tiledb/api/c_api/subarray/subarray_api_experimental.h
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
 * This file declares the subarray section of the experimental TileDB C API
 */

#ifndef TILEDB_CAPI_SUBARRAY_EXPERIMENTAL_H
#define TILEDB_CAPI_SUBARRAY_EXPERIMENTAL_H

#include "../api_external_common.h"
#include "subarray_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Adds point ranges to the given dimension index of the subarray
 * Effectively `add_range(x_i, x_i)` for `count` points in the
 * target array, but set in bulk to amortize expensive steps.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t ranges[] = {1, 3, 7, 10};
 * tiledb_subarray_add_point_ranges(ctx, subarray, 0, ranges, 4);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] subarray The subarray.
 * @param[in] dim_idx The index of the dimension to add the range to.
 * @param[in] start The range start.
 * @param[in] count The number of points in the target array to add ranges on.
 *
 */
TILEDB_EXPORT capi_return_t tiledb_subarray_add_point_ranges(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const void* start,
    uint64_t count) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_SUBARRAY_EXPERIMENTAL_H
