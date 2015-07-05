/**
 * @file   tiledb_IO.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file declares the C APIs for basic Input/Output operations with arrays.
 */

#ifndef __TILEDB_IO_H__
#define __TILEDB_IO_H__

#include "tiledb_ctx.h"

#ifdef __cplusplus
extern "C" {
#endif

#if (defined __GNUC__ && __GNUC__ >= 4) || defined __INTEL_COMPILER
#  define TILEDB_EXPORT __attribute__((visibility("default")))
#else
#  define TILEDB_EXPORT
#endif

/** 
 * Closes an array, cleaning its metadata from main memory.
 * @param tiledb_context The TileDB state.
 * @param ad The descriptor of the array to be closed
 * @return An array descriptor (>=0) or an error code (<0). The error code may
 * be one of the following:
 * TBD
 * @see TileDB_Context, tiledb_open_array
 */
TILEDB_EXPORT int tiledb_close_array(
    TileDB_CTX* tiledb_ctx,
    int ad);

/** 
 * Prepares an array for reading or writing, reading its matadata in main 
 * memory. It returns an **array descriptor**, which is used in subsequent array
 * operations. 
 * @param tiledb_context The TileDB state.
 * @param array_name The name of the array to be opened
 * @param mode The mode in which is the array is opened. Currently, the 
 * following  modes are supported:
 * - **r**: Read mode
 * - **w**: Write mode (if the array exists, its data are cleared)
 * - **a**: Append mode (used when updating the array)
 * @return An array descriptor (>=0) or an error code (<0). The error code may
 * be one of the following:
 * TBD
 * @see TileDB_Context, tiledb_close_array
 */
TILEDB_EXPORT int tiledb_open_array(
    TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const char* mode);

#undef TILEDB_EXPORT

#ifdef __cplusplus
}
#endif

#endif
