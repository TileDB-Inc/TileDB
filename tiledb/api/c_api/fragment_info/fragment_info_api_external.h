/**
 * @file tiledb/api/c_api/fragment_info/fragment_info_api_external.h
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
 * This file declares the FragmentInfo C API for TileDB.
 */

#ifndef TILEDB_CAPI_FRAGMENT_INFO_EXTERNAL_H
#define TILEDB_CAPI_FRAGMENT_INFO_EXTERNAL_H

#include "../api_external_common.h"
#include "../context/context_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/** C API carrier for a TileDB fragment info object. */
typedef struct tiledb_fragment_info_handle_t tiledb_fragment_info_t;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_FRAGMENT_INFO_EXTERNAL_H
