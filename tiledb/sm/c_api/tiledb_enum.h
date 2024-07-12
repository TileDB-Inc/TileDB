/*
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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

/**
 * NOTE: The values of these enums are serialized to the array schema and/or
 * fragment metadata. Therefore, the values below should never change,
 * otherwise backwards compatibility breaks.
 */

// clang-format is disabled on the first enum so that we can manually indent it
// properly.
// clang-format off
#ifdef TILEDB_ARRAY_TYPE_ENUM
    /** Dense array */
    TILEDB_ARRAY_TYPE_ENUM(DENSE) = 0,
    /** Sparse array */
    TILEDB_ARRAY_TYPE_ENUM(SPARSE) = 1,
#endif
// clang-format on

#ifdef TILEDB_LAYOUT_ENUM
    /** Row-major layout */
    TILEDB_LAYOUT_ENUM(ROW_MAJOR) = 0,
    /** Column-major layout */
    TILEDB_LAYOUT_ENUM(COL_MAJOR) = 1,
    /** Global-order layout */
    TILEDB_LAYOUT_ENUM(GLOBAL_ORDER) = 2,
    /** Unordered layout */
    TILEDB_LAYOUT_ENUM(UNORDERED) = 3,
    /** Hilbert layout */
    TILEDB_LAYOUT_ENUM(HILBERT) = 4,
#endif

#ifdef TILEDB_ENCRYPTION_TYPE_ENUM
    /** No encryption. */
    TILEDB_ENCRYPTION_TYPE_ENUM(NO_ENCRYPTION) = 0,
    /** AES-256-GCM encryption. */
    TILEDB_ENCRYPTION_TYPE_ENUM(AES_256_GCM) = 1,
#endif

#ifdef TILEDB_QUERY_STATUS_ENUM
    /** Query failed */
    TILEDB_QUERY_STATUS_ENUM(FAILED) = 0,
    /** Query completed (all data has been read) */
    TILEDB_QUERY_STATUS_ENUM(COMPLETED) = 1,
    /** Query is in progress */
    TILEDB_QUERY_STATUS_ENUM(INPROGRESS) = 2,
    /** Query completed (but not all data has been read) */
    TILEDB_QUERY_STATUS_ENUM(INCOMPLETE) = 3,
    /** Query not initialized.  */
    TILEDB_QUERY_STATUS_ENUM(UNINITIALIZED) = 4,
    /** Query initialized (strategy created)  */
    TILEDB_QUERY_STATUS_ENUM(INITIALIZED) = 5,
#endif

#ifdef TILEDB_QUERY_STATUS_DETAILS_ENUM
    TILEDB_QUERY_STATUS_DETAILS_ENUM(REASON_NONE) = 0,
    /** User buffers are too small */
    TILEDB_QUERY_STATUS_DETAILS_ENUM(REASON_USER_BUFFER_SIZE) = 1,
    /** Exceeded memory budget: can resubmit without resize */
    TILEDB_QUERY_STATUS_DETAILS_ENUM(REASON_MEMORY_BUDGET) = 2,
#endif

// This enumeration is special in that if you add enumeration entries here
// you have to manually add the new values in tiledb.h. This is to avoid
// exposing `TILEDB_ALWAYS_TRUE` and `TILEDB_ALWAYS_FALSE` in the public API.
#ifdef TILEDB_QUERY_CONDITION_OP_ENUM
    /** Less-than operator */
    TILEDB_QUERY_CONDITION_OP_ENUM(LT) = 0,
    /** Less-than-or-equal operator */
    TILEDB_QUERY_CONDITION_OP_ENUM(LE) = 1,
    /** Greater-than operator */
    TILEDB_QUERY_CONDITION_OP_ENUM(GT) = 2,
    /** Greater-than-or-equal operator */
    TILEDB_QUERY_CONDITION_OP_ENUM(GE) = 3,
    /** Equal operator */
    TILEDB_QUERY_CONDITION_OP_ENUM(EQ) = 4,
    /** Not-equal operator */
    TILEDB_QUERY_CONDITION_OP_ENUM(NE) = 5,
    /** IN set membership operator. */
    TILEDB_QUERY_CONDITION_OP_ENUM(IN) = 6,
    /** NOT IN set membership operator. */
    TILEDB_QUERY_CONDITION_OP_ENUM(NOT_IN) = 7,
    /** ALWAYS TRUE operator. */
    TILEDB_QUERY_CONDITION_OP_ENUM(ALWAYS_TRUE) = 253,
    /** ALWAYS TRUE operator. */
    TILEDB_QUERY_CONDITION_OP_ENUM(ALWAYS_FALSE) = 254,
#endif

#ifdef TILEDB_QUERY_CONDITION_COMBINATION_OP_ENUM
    /**'And' operator */
    TILEDB_QUERY_CONDITION_COMBINATION_OP_ENUM(AND) = 0,
    /** 'Or' operator */
    TILEDB_QUERY_CONDITION_COMBINATION_OP_ENUM(OR) = 1,
    /** 'Not' operator */
    TILEDB_QUERY_CONDITION_COMBINATION_OP_ENUM(NOT) = 2,
#endif

#ifdef TILEDB_SERIALIZATION_TYPE_ENUM
    /** Serialize to json */
    TILEDB_SERIALIZATION_TYPE_ENUM(JSON),
    /** Serialize to capnp */
    TILEDB_SERIALIZATION_TYPE_ENUM(CAPNP),
#endif

#ifdef TILEDB_MIME_TYPE_ENUM
    /** Unspecified MIME type*/
    TILEDB_MIME_TYPE_ENUM(MIME_AUTODETECT) = 0,
    /** image/tiff*/
    TILEDB_MIME_TYPE_ENUM(MIME_TIFF) = 1,
    /** application/pdf*/
    TILEDB_MIME_TYPE_ENUM(MIME_PDF) = 2,
#endif
