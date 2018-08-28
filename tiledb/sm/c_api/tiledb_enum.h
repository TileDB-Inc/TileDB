/*
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 */

// clang-format is disabled on the first enum so that we can manually indent it
// properly.
// clang-format off
#ifdef TILEDB_QUERY_TYPE_ENUM
    /** Read query */
    TILEDB_QUERY_TYPE_ENUM(READ),
    /** Write query */
    TILEDB_QUERY_TYPE_ENUM(WRITE),
#endif
// clang-format on

#ifdef TILEDB_OBJECT_TYPE_ENUM
    /** Invalid object */
    TILEDB_OBJECT_TYPE_ENUM(INVALID) = 1,
    /** Group object */
    TILEDB_OBJECT_TYPE_ENUM(GROUP),
    /** Array object */
    TILEDB_OBJECT_TYPE_ENUM(ARRAY),
    /** Key-value object */
    TILEDB_OBJECT_TYPE_ENUM(KEY_VALUE)
#endif

#ifdef TILEDB_FILESYSTEM_ENUM
    /** HDFS filesystem */
    TILEDB_FILESYSTEM_ENUM(HDFS),
    /** S3 filesystem */
    TILEDB_FILESYSTEM_ENUM(S3),
#endif

#ifdef TILEDB_DATATYPE_ENUM
    /** 32-bit signed integer */
    TILEDB_DATATYPE_ENUM(INT32),
    /** 64-bit signed integer */
    TILEDB_DATATYPE_ENUM(INT64),
    /** 32-bit floating point value */
    TILEDB_DATATYPE_ENUM(FLOAT32),
    /** 64-bit floating point value */
    TILEDB_DATATYPE_ENUM(FLOAT64),
    /** Character */
    TILEDB_DATATYPE_ENUM(CHAR),
    /** 8-bit signed integer */
    TILEDB_DATATYPE_ENUM(INT8),
    /** 8-bit unsigned integer */
    TILEDB_DATATYPE_ENUM(UINT8),
    /** 16-bit signed integer */
    TILEDB_DATATYPE_ENUM(INT16),
    /** 16-bit unsigned integer */
    TILEDB_DATATYPE_ENUM(UINT16),
    /** 32-bit unsigned integer */
    TILEDB_DATATYPE_ENUM(UINT32),
    /** 64-bit unsigned integer */
    TILEDB_DATATYPE_ENUM(UINT64),
    /** ASCII string */
    TILEDB_DATATYPE_ENUM(STRING_ASCII),
    /** UTF-8 string */
    TILEDB_DATATYPE_ENUM(STRING_UTF8),
    /** UTF-16 string */
    TILEDB_DATATYPE_ENUM(STRING_UTF16),
    /** UTF-32 string */
    TILEDB_DATATYPE_ENUM(STRING_UTF32),
    /** UCS2 string */
    TILEDB_DATATYPE_ENUM(STRING_UCS2),
    /** UCS4 string */
    TILEDB_DATATYPE_ENUM(STRING_UCS4),
    /** This can be any datatype. Must store (type tag, value) pairs. */
    TILEDB_DATATYPE_ENUM(ANY),
#endif

#ifdef TILEDB_ARRAY_TYPE_ENUM
    /** Dense array */
    TILEDB_ARRAY_TYPE_ENUM(DENSE),
    /** Sparse array */
    TILEDB_ARRAY_TYPE_ENUM(SPARSE),
#endif

#ifdef TILEDB_LAYOUT_ENUM
    /** Row-major layout */
    TILEDB_LAYOUT_ENUM(ROW_MAJOR),
    /** Column-major layout */
    TILEDB_LAYOUT_ENUM(COL_MAJOR),
    /** Global-order layout */
    TILEDB_LAYOUT_ENUM(GLOBAL_ORDER),
    /** Unordered layout */
    TILEDB_LAYOUT_ENUM(UNORDERED),
#endif

#ifdef TILEDB_COMPRESSOR_ENUM
#undef BLOSC_LZ4
#undef BLOSC_LZ4HC
#undef BLOSC_SNAPPY
#undef BLOSC_ZLIB
#undef BLOSC_ZSTD
    /** No compressor */
    TILEDB_COMPRESSOR_ENUM(NO_COMPRESSION),
    /** Gzip compressor */
    TILEDB_COMPRESSOR_ENUM(GZIP),
    /** Zstandard compressor */
    TILEDB_COMPRESSOR_ENUM(ZSTD),
    /** LZ4 compressor */
    TILEDB_COMPRESSOR_ENUM(LZ4),
    /** Blosc compressor using LZ */
    TILEDB_COMPRESSOR_ENUM(BLOSC_LZ),
    /** Blosc compressor using LZ4 */
    TILEDB_COMPRESSOR_ENUM(BLOSC_LZ4),
    /** Blosc compressor using LZ4HC */
    TILEDB_COMPRESSOR_ENUM(BLOSC_LZ4HC),
    /** Blosc compressor using Snappy */
    TILEDB_COMPRESSOR_ENUM(BLOSC_SNAPPY),
    /** Blosc compressor using zlib */
    TILEDB_COMPRESSOR_ENUM(BLOSC_ZLIB),
    /** Blosc compressor using Zstandard */
    TILEDB_COMPRESSOR_ENUM(BLOSC_ZSTD),
    /** Run-length encoding compressor */
    TILEDB_COMPRESSOR_ENUM(RLE),
    /** Bzip2 compressor */
    TILEDB_COMPRESSOR_ENUM(BZIP2),
    /** Double-delta compressor */
    TILEDB_COMPRESSOR_ENUM(DOUBLE_DELTA),
#endif

#ifdef TILEDB_FILTER_TYPE_ENUM
    /** Compression filter. */
    TILEDB_FILTER_TYPE_ENUM(COMPRESSION),
#endif

#ifdef TILEDB_QUERY_STATUS_ENUM
    /** Query failed */
    TILEDB_QUERY_STATUS_ENUM(FAILED) = -1,
    /** Query completed (all data has been read) */
    TILEDB_QUERY_STATUS_ENUM(COMPLETED) = 0,
    /** Query is in progress */
    TILEDB_QUERY_STATUS_ENUM(INPROGRESS) = 1,
    /** Query completed (but not all data has been read) */
    TILEDB_QUERY_STATUS_ENUM(INCOMPLETE) = 2,
    /** Query not initialized.  */
    TILEDB_QUERY_STATUS_ENUM(UNINITIALIZED) = 3,
#endif

#ifdef TILEDB_WALK_ORDER_ENUM
    /** Pre-order traversal */
    TILEDB_WALK_ORDER_ENUM(PREORDER),
    /** Post-order traversal */
    TILEDB_WALK_ORDER_ENUM(POSTORDER),
#endif

/** TileDB VFS mode */
#ifdef TILEDB_VFS_MODE_ENUM
    /** Read mode */
    TILEDB_VFS_MODE_ENUM(VFS_READ),
    /** Write mode */
    TILEDB_VFS_MODE_ENUM(VFS_WRITE),
    /** Append mode */
    TILEDB_VFS_MODE_ENUM(VFS_APPEND),
#endif
