/**
 * @file tiledb/api/c_api/fragment_info/fragment_info_api_experimental.h
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
 * This file declares the fragment info section of the experimental TileDB C API
 */

#ifndef TILEDB_CAPI_FRAGMENT_INFO_EXPERIMENTAL_H
#define TILEDB_CAPI_FRAGMENT_INFO_EXPERIMENTAL_H

#include "../api_external_common.h"
#include "fragment_info_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Retrieves the number of cells written to the fragments by the user.
 *
 * Contributions from each fragment to the total are as described in following.
 *
 * In the case of sparse fragments, this is the number of non-empty
 * cells in the fragment.
 *
 * In the case of dense fragments, TileDB may add fill
 * values to populate partially populated tiles. Those fill values
 * are counted in the returned number of cells. In other words,
 * the cell number is derived from the number of *integral* tiles
 * written in the file.
 *
 * note: The count returned is the cumulative total of cells
 * written to all fragments in the current fragment_info entity,
 * i.e. count may effectively include multiples for any cells that
 * may be overlapping across the various fragments.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t cell_num;
 * tiledb_fragment_info_get_total_cell_num(ctx, fragment_info, &cell_num);
 * @endcode
 *
 * @param[in]  ctx The TileDB context
 * @param[in]  fragment_info The fragment info object.
 * @param[out] count The number of cells to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_total_cell_num(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint64_t* count) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_FRAGMENT_INFO_EXPERIMENTAL_H
