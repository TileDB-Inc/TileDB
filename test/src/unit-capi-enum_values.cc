/**
 * @file   unit-capi-enum_values.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2025 TileDB Inc.
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

#include <test/support/tdb_catch.h>
#include "tiledb/api/c_api/array_schema/array_schema_api_external.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/enums/filter_type.h"

#ifdef TILEDB_SERIALIZATION
#include "tiledb/sm/c_api/tiledb_serialization.h"
#endif

#include <iostream>

using namespace tiledb::sm;

TEST_CASE("C API: Test enum values", "[capi][enums]") {
  /**
   * NOTE: The values of these enums are serialized to the array schema and/or
   * fragment metadata. Therefore, the values below should never change (i.e.
   * don't modify the checks below), otherwise backwards compatibility breaks.
   */

  /** Array type */
  REQUIRE(TILEDB_DENSE == 0);
  REQUIRE(TILEDB_SPARSE == 1);

  /** Layout type */
  REQUIRE(TILEDB_ROW_MAJOR == 0);
  REQUIRE(TILEDB_COL_MAJOR == 1);
  REQUIRE(TILEDB_GLOBAL_ORDER == 2);
  REQUIRE(TILEDB_UNORDERED == 3);

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
  REQUIRE(TILEDB_FILTER_CHECKSUM_MD5 == 12);
  REQUIRE(TILEDB_FILTER_CHECKSUM_SHA256 == 13);
  REQUIRE(TILEDB_FILTER_DICTIONARY == 14);
  REQUIRE(TILEDB_FILTER_SCALE_FLOAT == 15);
  REQUIRE(TILEDB_FILTER_XOR == 16);
  REQUIRE(TILEDB_FILTER_DEPRECATED == 17);
  REQUIRE(TILEDB_FILTER_WEBP == 18);
  REQUIRE(TILEDB_FILTER_DELTA == 19);
  REQUIRE(TILEDB_INTERNAL_FILTER_COUNT == 20);

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
  REQUIRE(TILEDB_INITIALIZED == 5);

  /** Walk order */
  REQUIRE(TILEDB_PREORDER == 0);
  REQUIRE(TILEDB_POSTORDER == 1);

  /** VFS mode */
  REQUIRE(TILEDB_VFS_READ == 0);
  REQUIRE(TILEDB_VFS_WRITE == 1);
  REQUIRE(TILEDB_VFS_APPEND == 2);
}

TEST_CASE("C API: Test enum string conversion", "[capi][enums]") {
  const char* c_str = nullptr;
  tiledb_array_type_t array_type;
  REQUIRE(
      (tiledb_array_type_to_str(TILEDB_DENSE, &c_str) == TILEDB_OK &&
       std::string(c_str) == "dense"));
  REQUIRE(
      (tiledb_array_type_from_str("dense", &array_type) == TILEDB_OK &&
       array_type == TILEDB_DENSE));
  REQUIRE(
      (tiledb_array_type_to_str(TILEDB_SPARSE, &c_str) == TILEDB_OK &&
       std::string(c_str) == "sparse"));
  REQUIRE(
      (tiledb_array_type_from_str("sparse", &array_type) == TILEDB_OK &&
       array_type == TILEDB_SPARSE));

  tiledb_layout_t layout;
  REQUIRE(
      (tiledb_layout_to_str(TILEDB_ROW_MAJOR, &c_str) == TILEDB_OK &&
       std::string(c_str) == "row-major"));
  REQUIRE(
      (tiledb_layout_from_str("row-major", &layout) == TILEDB_OK &&
       layout == TILEDB_ROW_MAJOR));
  REQUIRE(
      (tiledb_layout_to_str(TILEDB_COL_MAJOR, &c_str) == TILEDB_OK &&
       std::string(c_str) == "col-major"));
  REQUIRE(
      (tiledb_layout_from_str("col-major", &layout) == TILEDB_OK &&
       layout == TILEDB_COL_MAJOR));
  REQUIRE(
      (tiledb_layout_to_str(TILEDB_GLOBAL_ORDER, &c_str) == TILEDB_OK &&
       std::string(c_str) == "global-order"));
  REQUIRE(
      (tiledb_layout_from_str("global-order", &layout) == TILEDB_OK &&
       layout == TILEDB_GLOBAL_ORDER));
  REQUIRE(
      (tiledb_layout_to_str(TILEDB_UNORDERED, &c_str) == TILEDB_OK &&
       std::string(c_str) == "unordered"));
  REQUIRE(
      (tiledb_layout_from_str("unordered", &layout) == TILEDB_OK &&
       layout == TILEDB_UNORDERED));

  tiledb_filter_type_t filter_type;
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_NONE, &c_str) == TILEDB_OK &&
       std::string(c_str) == "NONE"));
  REQUIRE(
      (tiledb_filter_type_from_str("NONE", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_NONE));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_GZIP, &c_str) == TILEDB_OK &&
       std::string(c_str) == "GZIP"));
  REQUIRE(
      (tiledb_filter_type_from_str("GZIP", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_GZIP));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_ZSTD, &c_str) == TILEDB_OK &&
       std::string(c_str) == "ZSTD"));
  REQUIRE(
      (tiledb_filter_type_from_str("ZSTD", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_ZSTD));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_LZ4, &c_str) == TILEDB_OK &&
       std::string(c_str) == "LZ4"));
  REQUIRE(
      (tiledb_filter_type_from_str("LZ4", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_LZ4));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_RLE, &c_str) == TILEDB_OK &&
       std::string(c_str) == "RLE"));
  REQUIRE(
      (tiledb_filter_type_from_str("RLE", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_RLE));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_BZIP2, &c_str) == TILEDB_OK &&
       std::string(c_str) == "BZIP2"));
  REQUIRE(
      (tiledb_filter_type_from_str("BZIP2", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_BZIP2));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_DOUBLE_DELTA, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == "DOUBLE_DELTA"));
  REQUIRE(
      (tiledb_filter_type_from_str("DOUBLE_DELTA", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_DOUBLE_DELTA));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_BIT_WIDTH_REDUCTION, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == "BIT_WIDTH_REDUCTION"));
  REQUIRE(
      (tiledb_filter_type_from_str("BIT_WIDTH_REDUCTION", &filter_type) ==
           TILEDB_OK &&
       filter_type == TILEDB_FILTER_BIT_WIDTH_REDUCTION));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_BITSHUFFLE, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == "BITSHUFFLE"));
  REQUIRE(
      (tiledb_filter_type_from_str("BITSHUFFLE", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_BITSHUFFLE));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_BYTESHUFFLE, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == "BYTESHUFFLE"));
  REQUIRE(
      (tiledb_filter_type_from_str("BYTESHUFFLE", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_BYTESHUFFLE));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_POSITIVE_DELTA, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == "POSITIVE_DELTA"));
  REQUIRE(
      (tiledb_filter_type_from_str("POSITIVE_DELTA", &filter_type) ==
           TILEDB_OK &&
       filter_type == TILEDB_FILTER_POSITIVE_DELTA));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_CHECKSUM_MD5, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == "CHECKSUM_MD5"));
  REQUIRE(
      (tiledb_filter_type_from_str("CHECKSUM_MD5", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_CHECKSUM_MD5));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_CHECKSUM_SHA256, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == "CHECKSUM_SHA256"));
  REQUIRE(
      (tiledb_filter_type_from_str("CHECKSUM_SHA256", &filter_type) ==
           TILEDB_OK &&
       filter_type == TILEDB_FILTER_CHECKSUM_SHA256));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_DICTIONARY, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == "DICTIONARY_ENCODING"));
  REQUIRE(
      (tiledb_filter_type_from_str("DICTIONARY_ENCODING", &filter_type) ==
           TILEDB_OK &&
       filter_type == TILEDB_FILTER_DICTIONARY));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_SCALE_FLOAT, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == "SCALE_FLOAT"));
  REQUIRE(
      (tiledb_filter_type_from_str("SCALE_FLOAT", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_SCALE_FLOAT));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_XOR, &c_str) == TILEDB_OK &&
       std::string(c_str) == "XOR"));
  REQUIRE(
      (tiledb_filter_type_from_str("XOR", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_XOR));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_WEBP, &c_str) == TILEDB_OK &&
       std::string(c_str) == "WEBP"));
  REQUIRE(
      (tiledb_filter_type_from_str("WEBP", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_WEBP));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_DELTA, &c_str) == TILEDB_OK &&
       std::string(c_str) == "DELTA"));
  REQUIRE(
      (tiledb_filter_type_from_str("DELTA", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_DELTA));

  tiledb_filter_option_t filter_option;
  REQUIRE(
      (tiledb_filter_option_to_str(TILEDB_COMPRESSION_LEVEL, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == "COMPRESSION_LEVEL"));
  REQUIRE(
      (tiledb_filter_option_from_str("COMPRESSION_LEVEL", &filter_option) ==
           TILEDB_OK &&
       filter_option == TILEDB_COMPRESSION_LEVEL));
  REQUIRE(
      (tiledb_filter_option_to_str(TILEDB_BIT_WIDTH_MAX_WINDOW, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == "BIT_WIDTH_MAX_WINDOW"));
  REQUIRE(
      (tiledb_filter_option_from_str("BIT_WIDTH_MAX_WINDOW", &filter_option) ==
           TILEDB_OK &&
       filter_option == TILEDB_BIT_WIDTH_MAX_WINDOW));
  REQUIRE(
      (tiledb_filter_option_to_str(TILEDB_POSITIVE_DELTA_MAX_WINDOW, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == "POSITIVE_DELTA_MAX_WINDOW"));
  REQUIRE(
      (tiledb_filter_option_from_str(
           "POSITIVE_DELTA_MAX_WINDOW", &filter_option) == TILEDB_OK &&
       filter_option == TILEDB_POSITIVE_DELTA_MAX_WINDOW));
  REQUIRE(
      (tiledb_filter_option_from_str("SCALE_FLOAT_BYTEWIDTH", &filter_option) ==
           TILEDB_OK &&
       filter_option == TILEDB_SCALE_FLOAT_BYTEWIDTH));
  REQUIRE(
      (tiledb_filter_option_from_str("SCALE_FLOAT_FACTOR", &filter_option) ==
           TILEDB_OK &&
       filter_option == TILEDB_SCALE_FLOAT_FACTOR));
  REQUIRE(
      (tiledb_filter_option_from_str("SCALE_FLOAT_OFFSET", &filter_option) ==
           TILEDB_OK &&
       filter_option == TILEDB_SCALE_FLOAT_OFFSET));
  REQUIRE(
      (tiledb_filter_option_from_str("WEBP_QUALITY", &filter_option) ==
           TILEDB_OK &&
       filter_option == TILEDB_WEBP_QUALITY));
  REQUIRE(
      (tiledb_filter_option_from_str("WEBP_INPUT_FORMAT", &filter_option) ==
           TILEDB_OK &&
       filter_option == TILEDB_WEBP_INPUT_FORMAT));
  REQUIRE(
      (tiledb_filter_option_from_str("WEBP_LOSSLESS", &filter_option) ==
           TILEDB_OK &&
       filter_option == TILEDB_WEBP_LOSSLESS));
  REQUIRE(
      (tiledb_filter_option_to_str(
           TILEDB_COMPRESSION_REINTERPRET_DATATYPE, &c_str) == TILEDB_OK &&
       std::string(c_str) ==
           constants::filter_option_compression_reinterpret_datatype));
  REQUIRE(
      (tiledb_filter_option_from_str(
           "COMPRESSION_REINTERPRET_DATATYPE", &filter_option) == TILEDB_OK &&
       filter_option == TILEDB_COMPRESSION_REINTERPRET_DATATYPE));

  tiledb_encryption_type_t encryption_type;
  REQUIRE(
      (tiledb_encryption_type_to_str(TILEDB_NO_ENCRYPTION, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == "NO_ENCRYPTION"));
  REQUIRE(
      (tiledb_encryption_type_from_str("NO_ENCRYPTION", &encryption_type) ==
           TILEDB_OK &&
       encryption_type == TILEDB_NO_ENCRYPTION));
  REQUIRE(
      (tiledb_encryption_type_to_str(TILEDB_AES_256_GCM, &c_str) == TILEDB_OK &&
       std::string(c_str) == "AES_256_GCM"));
  REQUIRE(
      (tiledb_encryption_type_from_str("AES_256_GCM", &encryption_type) ==
           TILEDB_OK &&
       encryption_type == TILEDB_AES_256_GCM));

  tiledb_query_status_t query_status;
  REQUIRE(
      (tiledb_query_status_to_str(TILEDB_FAILED, &c_str) == TILEDB_OK &&
       std::string(c_str) == "FAILED"));
  REQUIRE(
      (tiledb_query_status_from_str("FAILED", &query_status) == TILEDB_OK &&
       query_status == TILEDB_FAILED));
  REQUIRE(
      (tiledb_query_status_to_str(TILEDB_COMPLETED, &c_str) == TILEDB_OK &&
       std::string(c_str) == "COMPLETED"));
  REQUIRE(
      (tiledb_query_status_from_str("COMPLETED", &query_status) == TILEDB_OK &&
       query_status == TILEDB_COMPLETED));
  REQUIRE(
      (tiledb_query_status_to_str(TILEDB_INPROGRESS, &c_str) == TILEDB_OK &&
       std::string(c_str) == "INPROGRESS"));
  REQUIRE(
      (tiledb_query_status_from_str("INPROGRESS", &query_status) == TILEDB_OK &&
       query_status == TILEDB_INPROGRESS));
  REQUIRE(
      (tiledb_query_status_to_str(TILEDB_INCOMPLETE, &c_str) == TILEDB_OK &&
       std::string(c_str) == "INCOMPLETE"));
  REQUIRE(
      (tiledb_query_status_from_str("INCOMPLETE", &query_status) == TILEDB_OK &&
       query_status == TILEDB_INCOMPLETE));
  REQUIRE(
      (tiledb_query_status_to_str(TILEDB_UNINITIALIZED, &c_str) == TILEDB_OK &&
       std::string(c_str) == "UNINITIALIZED"));
  REQUIRE(
      (tiledb_query_status_from_str("UNINITIALIZED", &query_status) ==
           TILEDB_OK &&
       query_status == TILEDB_UNINITIALIZED));
  REQUIRE(
      (tiledb_query_status_to_str(TILEDB_INITIALIZED, &c_str) == TILEDB_OK &&
       std::string(c_str) == "INITIALIZED"));
  REQUIRE((
      tiledb_query_status_from_str("INITIALIZED", &query_status) == TILEDB_OK &&
      query_status == TILEDB_INITIALIZED));

  tiledb_walk_order_t walk_order;
  REQUIRE(
      (tiledb_walk_order_to_str(TILEDB_PREORDER, &c_str) == TILEDB_OK &&
       std::string(c_str) == "PREORDER"));
  REQUIRE(
      (tiledb_walk_order_from_str("PREORDER", &walk_order) == TILEDB_OK &&
       walk_order == TILEDB_PREORDER));
  REQUIRE(
      (tiledb_walk_order_to_str(TILEDB_POSTORDER, &c_str) == TILEDB_OK &&
       std::string(c_str) == "POSTORDER"));
  REQUIRE(
      (tiledb_walk_order_from_str("POSTORDER", &walk_order) == TILEDB_OK &&
       walk_order == TILEDB_POSTORDER));

  tiledb_vfs_mode_t vfs_mode;
  REQUIRE(
      (tiledb_vfs_mode_to_str(TILEDB_VFS_READ, &c_str) == TILEDB_OK &&
       std::string(c_str) == "VFS_READ"));
  REQUIRE(
      (tiledb_vfs_mode_from_str("VFS_READ", &vfs_mode) == TILEDB_OK &&
       vfs_mode == TILEDB_VFS_READ));
  REQUIRE(
      (tiledb_vfs_mode_to_str(TILEDB_VFS_WRITE, &c_str) == TILEDB_OK &&
       std::string(c_str) == "VFS_WRITE"));
  REQUIRE(
      (tiledb_vfs_mode_from_str("VFS_WRITE", &vfs_mode) == TILEDB_OK &&
       vfs_mode == TILEDB_VFS_WRITE));
  REQUIRE(
      (tiledb_vfs_mode_to_str(TILEDB_VFS_APPEND, &c_str) == TILEDB_OK &&
       std::string(c_str) == "VFS_APPEND"));
  REQUIRE(
      (tiledb_vfs_mode_from_str("VFS_APPEND", &vfs_mode) == TILEDB_OK &&
       vfs_mode == TILEDB_VFS_APPEND));

#ifdef TILEDB_SERIALIZATION
  tiledb_serialization_type_t serialization_type;
  REQUIRE(
      (tiledb_serialization_type_to_str(TILEDB_JSON, &c_str) == TILEDB_OK &&
       std::string(c_str) == "JSON"));
  REQUIRE(
      (tiledb_serialization_type_from_str("JSON", &serialization_type) ==
           TILEDB_OK &&
       serialization_type == TILEDB_JSON));
  REQUIRE(
      (tiledb_serialization_type_to_str(TILEDB_CAPNP, &c_str) == TILEDB_OK &&
       std::string(c_str) == "CAPNP"));
  REQUIRE(
      (tiledb_serialization_type_from_str("CAPNP", &serialization_type) ==
           TILEDB_OK &&
       serialization_type == TILEDB_CAPNP));
#endif
}
