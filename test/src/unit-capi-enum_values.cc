/**
 * @file   unit-capi-enum_values.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB Inc.
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
 * Tests the specific values of C API enums.
 */

#include "catch.hpp"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/enums/filter_type.h"

#include <iostream>

using namespace tiledb::sm;

TEST_CASE("C API: Test enum values", "[capi], [enums]") {
  /**
   * NOTE: The values of these enums are serialized to the array schema and/or
   * fragment metadata. Therefore, the values below should never change (i.e.
   * don't modify the checks below), otherwise backwards compatibility breaks.
   */

  /** Query type */
  REQUIRE(TILEDB_READ == 0);
  REQUIRE(TILEDB_WRITE == 1);

  /** Object type */
  REQUIRE(TILEDB_INVALID == 0);
  REQUIRE(TILEDB_GROUP == 1);
  REQUIRE(TILEDB_ARRAY == 2);
  REQUIRE(TILEDB_KEY_VALUE == 3);

  /** Filesystem type */
  REQUIRE(TILEDB_HDFS == 0);
  REQUIRE(TILEDB_S3 == 1);

  /** Datatype */
  REQUIRE(TILEDB_INT32 == 0);
  REQUIRE(TILEDB_INT64 == 1);
  REQUIRE(TILEDB_FLOAT32 == 2);
  REQUIRE(TILEDB_FLOAT64 == 3);
  REQUIRE(TILEDB_CHAR == 4);
  REQUIRE(TILEDB_INT8 == 5);
  REQUIRE(TILEDB_UINT8 == 6);
  REQUIRE(TILEDB_INT16 == 7);
  REQUIRE(TILEDB_UINT16 == 8);
  REQUIRE(TILEDB_UINT32 == 9);
  REQUIRE(TILEDB_UINT64 == 10);
  REQUIRE(TILEDB_STRING_ASCII == 11);
  REQUIRE(TILEDB_STRING_UTF8 == 12);
  REQUIRE(TILEDB_STRING_UTF16 == 13);
  REQUIRE(TILEDB_STRING_UTF32 == 14);
  REQUIRE(TILEDB_STRING_UCS2 == 15);
  REQUIRE(TILEDB_STRING_UCS4 == 16);
  REQUIRE(TILEDB_ANY == 17);

  /** Array type */
  REQUIRE(TILEDB_DENSE == 0);
  REQUIRE(TILEDB_SPARSE == 1);

  /** Layout type */
  REQUIRE(TILEDB_ROW_MAJOR == 0);
  REQUIRE(TILEDB_COL_MAJOR == 1);
  REQUIRE(TILEDB_GLOBAL_ORDER == 2);
  REQUIRE(TILEDB_UNORDERED == 3);

  /** Compressor type */
  REQUIRE(TILEDB_NO_COMPRESSION == 0);
  REQUIRE(TILEDB_GZIP == 1);
  REQUIRE(TILEDB_ZSTD == 2);
  REQUIRE(TILEDB_LZ4 == 3);
  REQUIRE(TILEDB_RLE == 4);
  REQUIRE(TILEDB_BZIP2 == 5);
  REQUIRE(TILEDB_DOUBLE_DELTA == 6);

  /** Filter type */
  REQUIRE(TILEDB_FILTER_NONE == 0);
  REQUIRE(TILEDB_FILTER_GZIP == 1);
  REQUIRE(TILEDB_FILTER_ZSTD == 2);
  REQUIRE(TILEDB_FILTER_LZ4 == 3);
  REQUIRE(TILEDB_FILTER_RLE == 4);
  REQUIRE(TILEDB_FILTER_BZIP2 == 5);
  REQUIRE(TILEDB_FILTER_DOUBLE_DELTA == 6);
  REQUIRE(TILEDB_FILTER_BIT_WIDTH_REDUCTION == 7);
  REQUIRE(TILEDB_FILTER_BITSHUFFLE == 8);
  REQUIRE(TILEDB_FILTER_BYTESHUFFLE == 9);
  REQUIRE(TILEDB_FILTER_POSITIVE_DELTA == 10);
  REQUIRE((uint8_t)FilterType::INTERNAL_FILTER_AES_256_GCM == 11);

  /** Filter option */
  REQUIRE(TILEDB_COMPRESSION_LEVEL == 0);
  REQUIRE(TILEDB_BIT_WIDTH_MAX_WINDOW == 1);
  REQUIRE(TILEDB_POSITIVE_DELTA_MAX_WINDOW == 2);

  /** Encryption type */
  REQUIRE(TILEDB_NO_ENCRYPTION == 0);
  REQUIRE(TILEDB_AES_256_GCM == 1);

  /** Query status type */
  REQUIRE(TILEDB_FAILED == 0);
  REQUIRE(TILEDB_COMPLETED == 1);
  REQUIRE(TILEDB_INPROGRESS == 2);
  REQUIRE(TILEDB_INCOMPLETE == 3);
  REQUIRE(TILEDB_UNINITIALIZED == 4);

  /** Walk order */
  REQUIRE(TILEDB_PREORDER == 0);
  REQUIRE(TILEDB_POSTORDER == 1);

  /** VFS mode */
  REQUIRE(TILEDB_VFS_READ == 0);
  REQUIRE(TILEDB_VFS_WRITE == 1);
  REQUIRE(TILEDB_VFS_APPEND == 2);
}
