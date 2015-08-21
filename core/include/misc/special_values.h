/**
 * @file   special_values.h
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
 * This file defines some global special values.
 */

#ifndef SPECIAL_VALUES_H
#define SPECIAL_VALUES_H

#include <limits>

/** Name of the file storing the array schema. */
#define ARRAY_SCHEMA_FILENAME "array_schema"
/** Suffix of all book-keeping files. */
#define BOOK_KEEPING_FILE_SUFFIX ".bkp"
/** Name of the file storing the bounding coordinates of each tile. */
#define BOUNDING_COORDINATES_FILENAME "bounding_coordinates"
/** Deleted char. */
#define DEL_CHAR '$'
/** Deleted int. */
#define DEL_INT std::numeric_limits<int>::max()-1
/** Deleted int64_t. */
#define DEL_INT64_T std::numeric_limits<int64_t>::max()-1
/** Deleted float. */
#define DEL_FLOAT std::numeric_limits<float>::max()-1
/** Deleted double. */
#define DEL_DOUBLE std::numeric_limits<double>::max()-1
/** Deleted size_t. */
#define DEL_SIZE_T std::numeric_limits<size_t>::max()-1
/** Indicates a deleted value. */
#define DEL_VALUE '$'
/** Error log file name. */
#define ERROR_LOG_FILENAME "tiledb_error.log"
/** A header that precedes an error message. */
#define ERROR_MSG_HEADER "[TileDB] ERROR:"
/** Name of the file storing the fragment book-keeping info. */
#define FRAGMENT_TREE_FILENAME "fragment_tree"
/** Indicates an invalid tile position. */
#define INVALID_TILE_POS -1
/** Indicates an invalid tile id. */
#define INVALID_TILE_ID -1
/** Maximum number of arrays that can be simultaneously open. */
#define MAX_OPEN_ARRAYS 100
/** Name of the file storing the MBR of each tile. */
#define MBRS_FILENAME "mbrs"
/** TileDB message header */
#define MSG_HEADER "[TileDB]"
/** Indicates a missing (NULL) value. */
#define NULL_VALUE '*'
/** Missing char. */
#define NULL_CHAR '*'
/** Missing int. */
#define NULL_INT std::numeric_limits<int>::max()
/** Missing int64_t. */
#define NULL_INT64_T std::numeric_limits<int64_t>::max()
/** Missing float. */
#define NULL_FLOAT std::numeric_limits<float>::max()
/** Missing double. */
#define NULL_DOUBLE std::numeric_limits<double>::max()
/** Missing size_t. */
#define NULL_SIZE_T std::numeric_limits<size_t>::max()
/** Name of the file storing the offset of each tile in its data file. */
#define OFFSETS_FILENAME "offsets"
/** 
 * Determines the mount of data that can be exchanged between the hard disk and
 * the main memory in a single I/O operation. 
 */
#define SEGMENT_SIZE 10000000 // ~10MB
/** Name for temp (usually used in directory paths). */
#define TEMP "temp"
/** Name of the file storing the id of each tile. */
#define TILE_IDS_FILENAME "tile_ids"
/** Suffix of all tile data files. */
#define TILE_DATA_FILE_SUFFIX ".tdt"
/** Special value that indicates a variable-sized object. */
#define VAR_SIZE std::numeric_limits<int>::max()
/** Max memory size (in bytes) used when creating a new array fragment. */
#define WRITE_STATE_MAX_SIZE 1*1073741824 // 1GB

#endif
