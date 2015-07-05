/**
 * @file   tiledb_ctx.h
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
 * This file defines struct TileDB_CTX, which constitutes the TileDB
 * state that wraps the various TileDB modules. It also declares the
 * context initialization/finalization functions of TileDB.
 */

#ifndef __TILEDB_CTX_H__
#define __TILEDB_CTX_H__

#ifdef __cplusplus
extern "C" {
#endif

#if (defined __GNUC__ && __GNUC__ >= 4) || defined __INTEL_COMPILER
#  define TILEDB_EXPORT __attribute__((visibility("default")))
#else
#  define TILEDB_EXPORT
#endif

/**  Constitutes the TileDB state, wrapping the TileDB modules. */
typedef struct TileDB_CTX TileDB_CTX; 

/** 
 * Finalizes the TileDB context. On error, it prints a message on stderr and
 * returns an error code (shown below).
 * @param tiledb_context The TileDB state.
 * @return An error code, which can be one of the following:
 *   - **0**\n 
 *     Success
 * @see TileDB_Context, tiledb_init
 */
TILEDB_EXPORT int tiledb_finalize(TileDB_CTX*& tiledb_ctx);

/** 
 * Initializes the TileDB context. On error, it prints a message on stderr and
 * returns an error code (shown below). 
 * @param workspace The path to the workspace folder, i.e., the directory where
 * TileDB will store the array data. The workspace must exist, and the caller
 * must have read and write permissions to it.
 * @param tiledb_context The TileDB state.
 * @return An error code, which can be one of the following:
 *   - **0**\n 
 *     Success
 *   - **TILEDB_ENSMCREAT**\n 
 *     Failed to create storage manager
 *   - **TILEDB_ENLDCREAT**\n 
 *     Failed to create loader
 *   - **TILEDB_ENQPCREAT**\n 
 *     Failed to create query processor
 * @see TileDB_Context, tiledb_finalize
 */
TILEDB_EXPORT int tiledb_init(
    const char* workspace, 
    TileDB_CTX*& tiledb_ctx);

#undef TILEDB_EXPORT

#ifdef __cplusplus
}
#endif

#endif
