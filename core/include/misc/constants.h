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

/** Name of the coordinates attribute. */
#define TILEDB_COORDS_NAME "__coords"

/** Suffix of a TileDB file. */
#define TILEDB_FILE_SUFFIX ".tdb"

/** Suffix of a GZIP-compressed file. */
#define TILEDB_GZIP_SUFFIX ".gz"

// Special file names
#define TILEDB_ARRAY_SCHEMA_FILENAME   "__array_schema.tdb"
#define TILEDB_BOOK_KEEPING_FILENAME   "__book_keeping"
#define TILEDB_FRAGMENT_FILENAME       "__tiledb_fragment.tdb"
#define TILEDB_GROUP_FILENAME          "__tiledb_group.tdb"
#define TILEDB_WORKSPACE_FILENAME      "__tiledb_workspace.tdb"

/** The size of the sorted buffer. */
#define TILEDB_SORTED_BUFFER_SIZE 10000000  // ~10MB

/** The TileDB data types. */ 
enum DataType {
    TILEDB_CHAR, 
    TILEDB_INT32, 
    TILEDB_INT64, 
    TILEDB_FLOAT32, 
    TILEDB_FLOAT64
};

#endif
