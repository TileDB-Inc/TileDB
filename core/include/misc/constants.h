/**
 * @file   constants.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT, Intel Corporation and TileDB, Inc.
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
 * This file contains global TileDB constants.
 */

#ifndef __CONSTANTS_H__
#define __CONSTANTS_H__

#include <cfloat>
#include <climits>

namespace tiledb {

/**@{*/
/** Special empty cell value. */
#define TILEDB_EMPTY_INT32 INT_MAX
#define TILEDB_EMPTY_INT64 INT64_MAX
#define TILEDB_EMPTY_FLOAT32 FLT_MAX
#define TILEDB_EMPTY_FLOAT64 DBL_MAX
#define TILEDB_EMPTY_CHAR CHAR_MAX
#define TILEDB_EMPTY_INT8 INT8_MAX
#define TILEDB_EMPTY_UINT8 UINT8_MAX
#define TILEDB_EMPTY_INT16 INT16_MAX
#define TILEDB_EMPTY_UINT16 UINT16_MAX
#define TILEDB_EMPTY_UINT32 UINT32_MAX
#define TILEDB_EMPTY_UINT64 UINT64_MAX
/**@}*/

};  // tiledb namespace

#endif  // __CONSTANTS_H__