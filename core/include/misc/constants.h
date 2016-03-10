/**
 * @file   constants.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2015 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file contains global constants. 
 */

#ifndef __CONSTANTS_H__
#define __CONSTANTS_H__

#include <float.h>
#include <limits.h>

/** Version */
#define TILEDB_VERSION "0.1"

/* Return codes. */
#define TILEDB_OK                     0
#define TILEDB_ERR                   -1

/* Array modes. */
#define TILEDB_READ                   1
#define TILEDB_READ_REVERSE           2
#define TILEDB_WRITE                  3
#define TILEDB_WRITE_UNSORTED         4

/** Metadata modes. */
#define TILEDB_METADATA_READ  0
#define TILEDB_METADATA_WRITE 1

/** Special cell values. */
#define TILEDB_EMPTY_INT32 INT_MAX
#define TILEDB_EMPTY_INT64 LLONG_MAX
#define TILEDB_EMPTY_FLOAT32 FLT_MAX
#define TILEDB_EMPTY_FLOAT64 DBL_MAX
#define TILEDB_EMPTY_CHAR CHAR_MAX

// TODO
#define TILEDB_VAR_NUM       INT_MAX

/** Types */
#define TILEDB_INT32    0
#define TILEDB_INT64    1
#define TILEDB_FLOAT32  2
#define TILEDB_FLOAT64  3
#define TILEDB_CHAR     4

/** Tile and cell orders. */
#define TILEDB_ROW_MAJOR 0
#define TILEDB_COL_MAJOR 1
#define TILEDB_HILBERT   2

/** Compression */
#define TILEDB_NO_COMPRESSION    0
#define TILEDB_GZIP              1

/** Name of the coordinates attribute. */
#define TILEDB_COORDS_NAME "__coords"

// TODO
#define TILEDB_KEY_NAME "__key"
#define TILEDB_KEY_DIM1_NAME "__key_dim_1"
#define TILEDB_KEY_DIM2_NAME "__key_dim_2"
#define TILEDB_KEY_DIM3_NAME "__key_dim_3"
#define TILEDB_KEY_DIM4_NAME "__key_dim_4"

/** Suffix of a TileDB file. */
#define TILEDB_FILE_SUFFIX ".tdb"

/** Suffix of a GZIP-compressed file. */
#define TILEDB_GZIP_SUFFIX ".gz"

/** Chunk size in GZIP decompression. */
#define TILEDB_GZIP_CHUNK_SIZE 131072 // 128KB

// Special file names
#define TILEDB_ARRAY_SCHEMA_FILENAME      "__array_schema.tdb"
#define TILEDB_METADATA_SCHEMA_FILENAME   "__metadata_schema.tdb"
#define TILEDB_BOOK_KEEPING_FILENAME      "__book_keeping"
#define TILEDB_FRAGMENT_FILENAME          "__tiledb_fragment.tdb"
#define TILEDB_GROUP_FILENAME             "__tiledb_group.tdb"
#define TILEDB_WORKSPACE_FILENAME         "__tiledb_workspace.tdb"

/** The size of the sorted buffer. */
#define TILEDB_SORTED_BUFFER_SIZE 10000000  // ~10MB
/** The size of the sorted buffer for varibale attributes. */
#define TILEDB_SORTED_BUFFER_VAR_SIZE 10000000  // ~10MB

/** Size of the starting offset of a variable cell. */
#define TILEDB_CELL_VAR_OFFSET_SIZE sizeof(size_t)

#endif
