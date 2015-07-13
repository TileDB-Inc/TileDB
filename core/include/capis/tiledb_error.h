/**
 * @file   tiledb_error.h
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
 * This file defines the error codes and messages, as well as the error
 * functions.
 */

#ifndef __TILEDB_ERROR_H__
#define __TILEDB_ERROR_H__

#define TILEDB_OK 0
#define TILEDB_OK_STR "No error"

#define TILEDB_EPARSE -1
#define TILEDB_EPARSE_STR "Parser error"

#define TILEDB_ENDEFARR -2
#define TILEDB_ENDEFARR_STR "Undefined array"

#define TILEDB_EFILE -3
#define TILEDB_EFILE_STR "File operation failed"

#define TILEDB_ENSMCREAT -4
#define TILEDB_ENSMCREAT_STR "Failed to create storage manager"

#define TILEDB_ENLDCREAT -5
#define TILEDB_ENLDCREAT_STR "Failed to create loader"

#define TILEDB_ENQPCREAT -6
#define TILEDB_ENQPCREAT_STR "Failed to create query processor"

#define TILEDB_EINIT -7
#define TILEDB_EINIT_STR "Failed to initialize TileDB"

#define TILEDB_EFIN -8
#define TILEDB_EFIN_STR "Failed to finalize TileDB"

#define TILEDB_EPARRSCHEMA -9
#define TILEDB_EPARRSCHEMA_STR "Failed to parse array schema"

#define TILEDB_EDNEXIST -10
#define TILEDB_EDNEXIST_STR "Directory does not exist"

#define TILEDB_EDNCREAT -11
#define TILEDB_EDNCREAT_STR "Failed to create directory"

#define TILEDB_ERARRSCHEMA -12
#define TILEDB_ERARRSCHEMA_STR "Failed to retrieve array schema"

#define TILEDB_EDEFARR -13
#define TILEDB_EDEFARR_STR "Failed to define array"

#define TILEDB_EOARR -14
#define TILEDB_EOARR_STR "Failed to open array"

#define TILEDB_ECARR -15
#define TILEDB_ECARR_STR "Failed to close array"

#define TILEDB_EIARG -16
#define TILEDB_EIARG_STR "Invalid argument"

#ifdef __cplusplus
extern "C" {
#endif

#if (defined __GNUC__ && __GNUC__ >= 4) || defined __INTEL_COMPILER
#  define TILEDB_EXPORT __attribute__((visibility("default")))
#else
#  define TILEDB_EXPORT
#endif

/** 
 * Returns the description corresponding to the input error code. 
 * @param err An error code (see definitions above).
 * @return The description of the error.
 */
TILEDB_EXPORT const char* tiledb_strerror(int err); 

#undef TILEDB_EXPORT

#ifdef __cplusplus
}
#endif

#endif
