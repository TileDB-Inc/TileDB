/**
 * @file   tiledb_constants.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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

#ifndef __TILEDB_CONSTANTS_H__
#define __TILEDB_CONSTANTS_H__

#include <cfloat>
#include <climits>

/** Version. */
#define TILEDB_VERSION "0.6.1"
#define TILEDB_VERSION_MAJOR 0
#define TILEDB_VERSION_MINOR 6
#define TILEDB_VERSION_REVISION 1

/**@{*/
/** Return code. */
#define TILEDB_OK 0
#define TILEDB_ERR -1
#define TILEDB_OOM -2
/**@}*/

/**
 * The TileDB home directory, where TileDB-related system metadata structures
 * are kept. If it is set to "", then the home directory is set to "~/.tiledb"
 * by default.
 */
#define TILEDB_HOME ""

/** The maximum length for the names of TileDB objects. */
#define TILEDB_NAME_MAX_LEN 256

/** Size of the buffer used during consolidation. */
#define TILEDB_CONSOLIDATION_BUFFER_SIZE 10000000  // ~10 MB

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

/**@{*/
/** Special value indicating a variable number or size. */
#define TILEDB_VAR_NUM INT_MAX
#define TILEDB_VAR_SIZE (size_t) - 1
/**@}*/

/**@{*/
/** Special attribute name. */
#define TILEDB_COORDS "__coords"
#define TILEDB_KEY "__key"
/**@}*/

/**@{*/
/** Special TileDB file name suffix. */
#define TILEDB_FILE_SUFFIX ".tdb"
#define TILEDB_GZIP_SUFFIX ".gz"
/**@}*/

/** Chunk size in GZIP decompression. */
#define TILEDB_GZIP_CHUNK_SIZE 131072  // 128KB

/**@{*/
/** Special TileDB file name. */
#define TILEDB_ARRAY_SCHEMA_FILENAME "__array_schema.tdb"
#define TILEDB_METADATA_SCHEMA_FILENAME "__metadata_schema.tdb"
#define TILEDB_BOOK_KEEPING_FILENAME "__book_keeping"
#define TILEDB_FRAGMENT_FILENAME "__tiledb_fragment.tdb"
#define TILEDB_GROUP_FILENAME "__tiledb_group.tdb"
#define TILEDB_WORKSPACE_FILENAME "__tiledb_workspace.tdb"
/**@}*/

/**@{*/
/** Size of buffer used for sorting. */
#define TILEDB_SORTED_BUFFER_SIZE 10000000      // ~10MB
#define TILEDB_SORTED_BUFFER_VAR_SIZE 10000000  // ~10MB
/**@}*/

/**@{*/
/** Compression levels. */
#ifndef TILEDB_COMPRESSION_LEVEL_GZIP
#define TILEDB_COMPRESSION_LEVEL_GZIP Z_DEFAULT_COMPRESSION
#endif
#ifndef TILEDB_COMPRESSION_LEVEL_ZSTD
#define TILEDB_COMPRESSION_LEVEL_ZSTD 1
#endif
#ifndef TILEDB_COMPRESSION_LEVEL_BLOSC
#define TILEDB_COMPRESSION_LEVEL_BLOSC 5
#endif
/**@}*/

/**@{*/
/** MAC address interface. */
#if defined(__APPLE__) && defined(__MACH__)
#ifndef TILEDB_MAC_ADDRESS_INTERFACE
#define TILEDB_MAC_ADDRESS_INTERFACE en0
#endif
#else
#ifndef TILEDB_MAC_ADDRESS_INTERFACE
#define TILEDB_MAC_ADDRESS_INTERFACE eth0
#endif
#endif
/**@}*/

#endif
