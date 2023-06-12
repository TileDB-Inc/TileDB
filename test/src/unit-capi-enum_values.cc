/**
 * @file   unit-capi-enum_values.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB Inc.
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
  REQUIRE(TILEDB_FILTER_TYPED_VIEW == 20);

  /** Filter option */
  REQUIRE(TILEDB_COMPRESSION_LEVEL == 0);
  REQUIRE(TILEDB_BIT_WIDTH_MAX_WINDOW == 1);
  REQUIRE(TILEDB_POSITIVE_DELTA_MAX_WINDOW == 2);
  REQUIRE(TILEDB_SCALE_FLOAT_BYTEWIDTH == 3);
  REQUIRE(TILEDB_SCALE_FLOAT_FACTOR == 4);
  REQUIRE(TILEDB_SCALE_FLOAT_OFFSET == 5);
  REQUIRE(TILEDB_WEBP_QUALITY == 6);
  REQUIRE(TILEDB_WEBP_INPUT_FORMAT == 7);
  REQUIRE(TILEDB_WEBP_LOSSLESS == 8);
  REQUIRE(TILEDB_TYPED_VIEW_FILTERED_DATATYPE == 9);
  REQUIRE(TILEDB_TYPED_VIEW_UNFILTERED_DATATYPE == 10);

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
       std::string(c_str) == constants::dense_str));
  REQUIRE(
      (tiledb_array_type_from_str("dense", &array_type) == TILEDB_OK &&
       array_type == TILEDB_DENSE));
  REQUIRE(
      (tiledb_array_type_to_str(TILEDB_SPARSE, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::sparse_str));
  REQUIRE(
      (tiledb_array_type_from_str("sparse", &array_type) == TILEDB_OK &&
       array_type == TILEDB_SPARSE));

  tiledb_layout_t layout;
  REQUIRE(
      (tiledb_layout_to_str(TILEDB_ROW_MAJOR, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::row_major_str));
  REQUIRE(
      (tiledb_layout_from_str("row-major", &layout) == TILEDB_OK &&
       layout == TILEDB_ROW_MAJOR));
  REQUIRE(
      (tiledb_layout_to_str(TILEDB_COL_MAJOR, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::col_major_str));
  REQUIRE(
      (tiledb_layout_from_str("col-major", &layout) == TILEDB_OK &&
       layout == TILEDB_COL_MAJOR));
  REQUIRE(
      (tiledb_layout_to_str(TILEDB_GLOBAL_ORDER, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::global_order_str));
  REQUIRE(
      (tiledb_layout_from_str("global-order", &layout) == TILEDB_OK &&
       layout == TILEDB_GLOBAL_ORDER));
  REQUIRE(
      (tiledb_layout_to_str(TILEDB_UNORDERED, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::unordered_str));
  REQUIRE(
      (tiledb_layout_from_str("unordered", &layout) == TILEDB_OK &&
       layout == TILEDB_UNORDERED));

  tiledb_filter_type_t filter_type;
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_NONE, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::filter_none_str));
  REQUIRE(
      (tiledb_filter_type_from_str("NONE", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_NONE));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_GZIP, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::gzip_str));
  REQUIRE(
      (tiledb_filter_type_from_str("GZIP", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_GZIP));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_ZSTD, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::zstd_str));
  REQUIRE(
      (tiledb_filter_type_from_str("ZSTD", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_ZSTD));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_LZ4, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::lz4_str));
  REQUIRE(
      (tiledb_filter_type_from_str("LZ4", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_LZ4));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_RLE, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::rle_str));
  REQUIRE(
      (tiledb_filter_type_from_str("RLE", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_RLE));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_BZIP2, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::bzip2_str));
  REQUIRE(
      (tiledb_filter_type_from_str("BZIP2", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_BZIP2));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_DOUBLE_DELTA, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == constants::double_delta_str));
  REQUIRE(
      (tiledb_filter_type_from_str("DOUBLE_DELTA", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_DOUBLE_DELTA));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_BIT_WIDTH_REDUCTION, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == constants::filter_bit_width_reduction_str));
  REQUIRE(
      (tiledb_filter_type_from_str("BIT_WIDTH_REDUCTION", &filter_type) ==
           TILEDB_OK &&
       filter_type == TILEDB_FILTER_BIT_WIDTH_REDUCTION));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_BITSHUFFLE, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == constants::filter_bitshuffle_str));
  REQUIRE(
      (tiledb_filter_type_from_str("BITSHUFFLE", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_BITSHUFFLE));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_BYTESHUFFLE, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == constants::filter_byteshuffle_str));
  REQUIRE(
      (tiledb_filter_type_from_str("BYTESHUFFLE", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_BYTESHUFFLE));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_POSITIVE_DELTA, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == constants::filter_positive_delta_str));
  REQUIRE(
      (tiledb_filter_type_from_str("POSITIVE_DELTA", &filter_type) ==
           TILEDB_OK &&
       filter_type == TILEDB_FILTER_POSITIVE_DELTA));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_CHECKSUM_MD5, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == constants::filter_checksum_md5_str));
  REQUIRE(
      (tiledb_filter_type_from_str("CHECKSUM_MD5", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_CHECKSUM_MD5));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_CHECKSUM_SHA256, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == constants::filter_checksum_sha256_str));
  REQUIRE(
      (tiledb_filter_type_from_str("CHECKSUM_SHA256", &filter_type) ==
           TILEDB_OK &&
       filter_type == TILEDB_FILTER_CHECKSUM_SHA256));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_DICTIONARY, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == constants::filter_dictionary_str));
  REQUIRE(
      (tiledb_filter_type_from_str("DICTIONARY_ENCODING", &filter_type) ==
           TILEDB_OK &&
       filter_type == TILEDB_FILTER_DICTIONARY));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_SCALE_FLOAT, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == constants::filter_scale_float_str));
  REQUIRE(
      (tiledb_filter_type_from_str("SCALE_FLOAT", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_SCALE_FLOAT));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_XOR, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::filter_xor_str));
  REQUIRE(
      (tiledb_filter_type_from_str("XOR", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_XOR));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_WEBP, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::filter_webp_str));
  REQUIRE(
      (tiledb_filter_type_from_str("WEBP", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_WEBP));
  REQUIRE(
      (tiledb_filter_type_to_str(TILEDB_FILTER_TYPED_VIEW, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == constants::filter_typed_view_str));
  REQUIRE(
      (tiledb_filter_type_from_str("TYPED_VIEW", &filter_type) == TILEDB_OK &&
       filter_type == TILEDB_FILTER_TYPED_VIEW));

  tiledb_filter_option_t filter_option;
  REQUIRE(
      (tiledb_filter_option_to_str(TILEDB_COMPRESSION_LEVEL, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == constants::filter_option_compression_level_str));
  REQUIRE(
      (tiledb_filter_option_from_str("COMPRESSION_LEVEL", &filter_option) ==
           TILEDB_OK &&
       filter_option == TILEDB_COMPRESSION_LEVEL));
  REQUIRE((
      tiledb_filter_option_to_str(TILEDB_BIT_WIDTH_MAX_WINDOW, &c_str) ==
          TILEDB_OK &&
      std::string(c_str) == constants::filter_option_bit_width_max_window_str));
  REQUIRE(
      (tiledb_filter_option_from_str("BIT_WIDTH_MAX_WINDOW", &filter_option) ==
           TILEDB_OK &&
       filter_option == TILEDB_BIT_WIDTH_MAX_WINDOW));
  REQUIRE(
      (tiledb_filter_option_to_str(TILEDB_POSITIVE_DELTA_MAX_WINDOW, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) ==
           constants::filter_option_positive_delta_max_window_str));
  REQUIRE(
      (tiledb_filter_option_from_str(
           "POSITIVE_DELTA_MAX_WINDOW", &filter_option) == TILEDB_OK &&
       filter_option == TILEDB_POSITIVE_DELTA_MAX_WINDOW));
  REQUIRE(
      (tiledb_filter_option_to_str(TILEDB_SCALE_FLOAT_BYTEWIDTH, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == constants::filter_option_scale_float_bytewidth));
  REQUIRE(
      (tiledb_filter_option_from_str("SCALE_FLOAT_BYTEWIDTH", &filter_option) ==
           TILEDB_OK &&
       filter_option == TILEDB_SCALE_FLOAT_BYTEWIDTH));
  REQUIRE(
      (tiledb_filter_option_to_str(TILEDB_SCALE_FLOAT_FACTOR, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == constants::filter_option_scale_float_factor));
  REQUIRE(
      (tiledb_filter_option_from_str("SCALE_FLOAT_FACTOR", &filter_option) ==
           TILEDB_OK &&
       filter_option == TILEDB_SCALE_FLOAT_FACTOR));
  REQUIRE(
      (tiledb_filter_option_to_str(TILEDB_SCALE_FLOAT_OFFSET, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == constants::filter_option_scale_float_offset));
  REQUIRE(
      (tiledb_filter_option_from_str("SCALE_FLOAT_OFFSET", &filter_option) ==
           TILEDB_OK &&
       filter_option == TILEDB_SCALE_FLOAT_OFFSET));
  REQUIRE(
      (tiledb_filter_option_to_str(TILEDB_WEBP_QUALITY, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::filter_option_webp_quality));
  REQUIRE(
      (tiledb_filter_option_from_str("WEBP_QUALITY", &filter_option) ==
           TILEDB_OK &&
       filter_option == TILEDB_WEBP_QUALITY));
  REQUIRE(
      (tiledb_filter_option_to_str(TILEDB_WEBP_INPUT_FORMAT, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == constants::filter_option_webp_input_format));
  REQUIRE(
      (tiledb_filter_option_from_str("WEBP_INPUT_FORMAT", &filter_option) ==
           TILEDB_OK &&
       filter_option == TILEDB_WEBP_INPUT_FORMAT));
  REQUIRE(
      (tiledb_filter_option_to_str(TILEDB_WEBP_LOSSLESS, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::filter_option_webp_lossless));
  REQUIRE(
      (tiledb_filter_option_from_str("WEBP_LOSSLESS", &filter_option) ==
           TILEDB_OK &&
       filter_option == TILEDB_WEBP_LOSSLESS));
  REQUIRE(
      (tiledb_filter_option_to_str(
           TILEDB_TYPED_VIEW_FILTERED_DATATYPE, &c_str) == TILEDB_OK &&
       std::string(c_str) ==
           constants::filter_option_typed_view_filtered_datatype));
  REQUIRE(
      (tiledb_filter_option_from_str(
           "TYPED_VIEW_FILTERED_DATATYPE", &filter_option) == TILEDB_OK &&
       filter_option == TILEDB_TYPED_VIEW_FILTERED_DATATYPE));
  REQUIRE(
      (tiledb_filter_option_to_str(
           TILEDB_TYPED_VIEW_UNFILTERED_DATATYPE, &c_str) == TILEDB_OK &&
       std::string(c_str) ==
           constants::filter_option_typed_view_unfiltered_datatype));
  REQUIRE(
      (tiledb_filter_option_from_str(
           "TYPED_VIEW_UNFILTERED_DATATYPE", &filter_option) == TILEDB_OK &&
       filter_option == TILEDB_TYPED_VIEW_UNFILTERED_DATATYPE));

  tiledb_encryption_type_t encryption_type;
  REQUIRE(
      (tiledb_encryption_type_to_str(TILEDB_NO_ENCRYPTION, &c_str) ==
           TILEDB_OK &&
       std::string(c_str) == constants::no_encryption_str));
  REQUIRE(
      (tiledb_encryption_type_from_str("NO_ENCRYPTION", &encryption_type) ==
           TILEDB_OK &&
       encryption_type == TILEDB_NO_ENCRYPTION));
  REQUIRE(
      (tiledb_encryption_type_to_str(TILEDB_AES_256_GCM, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::aes_256_gcm_str));
  REQUIRE(
      (tiledb_encryption_type_from_str("AES_256_GCM", &encryption_type) ==
           TILEDB_OK &&
       encryption_type == TILEDB_AES_256_GCM));

  tiledb_query_status_t query_status;
  REQUIRE(
      (tiledb_query_status_to_str(TILEDB_FAILED, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::query_status_failed_str));
  REQUIRE(
      (tiledb_query_status_from_str("FAILED", &query_status) == TILEDB_OK &&
       query_status == TILEDB_FAILED));
  REQUIRE(
      (tiledb_query_status_to_str(TILEDB_COMPLETED, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::query_status_completed_str));
  REQUIRE(
      (tiledb_query_status_from_str("COMPLETED", &query_status) == TILEDB_OK &&
       query_status == TILEDB_COMPLETED));
  REQUIRE(
      (tiledb_query_status_to_str(TILEDB_INPROGRESS, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::query_status_inprogress_str));
  REQUIRE(
      (tiledb_query_status_from_str("INPROGRESS", &query_status) == TILEDB_OK &&
       query_status == TILEDB_INPROGRESS));
  REQUIRE(
      (tiledb_query_status_to_str(TILEDB_INCOMPLETE, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::query_status_incomplete_str));
  REQUIRE(
      (tiledb_query_status_from_str("INCOMPLETE", &query_status) == TILEDB_OK &&
       query_status == TILEDB_INCOMPLETE));
  REQUIRE(
      (tiledb_query_status_to_str(TILEDB_UNINITIALIZED, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::query_status_uninitialized_str));
  REQUIRE(
      (tiledb_query_status_from_str("UNINITIALIZED", &query_status) ==
           TILEDB_OK &&
       query_status == TILEDB_UNINITIALIZED));
  REQUIRE(
      (tiledb_query_status_to_str(TILEDB_INITIALIZED, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::query_status_initialized_str));
  REQUIRE((
      tiledb_query_status_from_str("INITIALIZED", &query_status) == TILEDB_OK &&
      query_status == TILEDB_INITIALIZED));

  tiledb_walk_order_t walk_order;
  REQUIRE(
      (tiledb_walk_order_to_str(TILEDB_PREORDER, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::walkorder_preorder_str));
  REQUIRE(
      (tiledb_walk_order_from_str("PREORDER", &walk_order) == TILEDB_OK &&
       walk_order == TILEDB_PREORDER));
  REQUIRE(
      (tiledb_walk_order_to_str(TILEDB_POSTORDER, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::walkorder_postorder_str));
  REQUIRE(
      (tiledb_walk_order_from_str("POSTORDER", &walk_order) == TILEDB_OK &&
       walk_order == TILEDB_POSTORDER));

  tiledb_vfs_mode_t vfs_mode;
  REQUIRE(
      (tiledb_vfs_mode_to_str(TILEDB_VFS_READ, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::vfsmode_read_str));
  REQUIRE(
      (tiledb_vfs_mode_from_str("VFS_READ", &vfs_mode) == TILEDB_OK &&
       vfs_mode == TILEDB_VFS_READ));
  REQUIRE(
      (tiledb_vfs_mode_to_str(TILEDB_VFS_WRITE, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::vfsmode_write_str));
  REQUIRE(
      (tiledb_vfs_mode_from_str("VFS_WRITE", &vfs_mode) == TILEDB_OK &&
       vfs_mode == TILEDB_VFS_WRITE));
  REQUIRE(
      (tiledb_vfs_mode_to_str(TILEDB_VFS_APPEND, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::vfsmode_append_str));
  REQUIRE(
      (tiledb_vfs_mode_from_str("VFS_APPEND", &vfs_mode) == TILEDB_OK &&
       vfs_mode == TILEDB_VFS_APPEND));

#ifdef TILEDB_SERIALIZATION
  tiledb_serialization_type_t serialization_type;
  REQUIRE(
      (tiledb_serialization_type_to_str(TILEDB_JSON, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::serialization_type_json_str));
  REQUIRE(
      (tiledb_serialization_type_from_str("JSON", &serialization_type) ==
           TILEDB_OK &&
       serialization_type == TILEDB_JSON));
  REQUIRE(
      (tiledb_serialization_type_to_str(TILEDB_CAPNP, &c_str) == TILEDB_OK &&
       std::string(c_str) == constants::serialization_type_capnp_str));
  REQUIRE(
      (tiledb_serialization_type_from_str("CAPNP", &serialization_type) ==
           TILEDB_OK &&
       serialization_type == TILEDB_CAPNP));
#endif
}
