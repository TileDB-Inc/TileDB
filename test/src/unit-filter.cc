/**
 * @file unit-filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * Tests the `Filter` class.
 */

#include "test/src/helpers.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/bit_width_reduction_filter.h"
#include "tiledb/sm/filter/bitshuffle_filter.h"
#include "tiledb/sm/filter/byteshuffle_filter.h"
#include "tiledb/sm/filter/checksum_md5_filter.h"
#include "tiledb/sm/filter/checksum_sha256_filter.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/encryption_aes256gcm_filter.h"
#include "tiledb/sm/filter/filter_create.h"
#include "tiledb/sm/filter/noop_filter.h"
#include "tiledb/sm/filter/positive_delta_filter.h"

#include <catch.hpp>
#include <iostream>

using namespace tiledb::sm;

TEST_CASE(
    "Filter: Test bit width reduction filter deserialization",
    "[filter][bit-width-reduction]") {
  Buffer buffer;
  FilterType filtertype0 = FilterType::FILTER_BIT_WIDTH_REDUCTION;
  auto type = static_cast<uint8_t>(filtertype0);
  REQUIRE(buffer.write(&type, sizeof(uint8_t)).ok());

  // Save a pointer to store the actual length of the metadata.
  uint32_t metadata_len = 0;
  auto metadata_length_offset = buffer.offset();
  REQUIRE(buffer.write(&metadata_len, sizeof(uint32_t)).ok());

  // Filter-specific serialization
  uint64_t buff_size = buffer.size();

  uint32_t max_window_size0 = 1024;
  REQUIRE((buffer.write(&max_window_size0, sizeof(uint32_t))).ok());

  metadata_len = static_cast<uint32_t>(buffer.size() - buff_size);
  std::memcpy(
      buffer.data(metadata_length_offset), &metadata_len, sizeof(uint32_t));

  ConstBuffer constbuffer(&buffer);
  auto&& [st_filter, filter1]{FilterCreate::deserialize(&constbuffer)};
  REQUIRE(st_filter.ok());

  // Check type
  CHECK(filter1.value()->type() == filtertype0);

  uint32_t max_window_size1 = 0;
  REQUIRE(
      filter1.value()
          ->get_option(FilterOption::BIT_WIDTH_MAX_WINDOW, &max_window_size1)
          .ok());
  CHECK(max_window_size0 == max_window_size1);
}

TEST_CASE(
    "Filter: Test bit shuffle filter deserialization",
    "[filter][bit-shuffle]") {
  Buffer buffer;
  FilterType filtertype0 = FilterType::FILTER_BITSHUFFLE;
  auto type = static_cast<uint8_t>(filtertype0);
  REQUIRE(buffer.write(&type, sizeof(uint8_t)).ok());

  // Save a pointer to store the actual length of the metadata.
  uint32_t metadata_len = 0;
  auto metadata_length_offset = buffer.offset();
  REQUIRE(buffer.write(&metadata_len, sizeof(uint32_t)).ok());

  // Filter-specific serialization
  uint64_t buff_size = buffer.size();

  metadata_len = static_cast<uint32_t>(buffer.size() - buff_size);
  std::memcpy(
      buffer.data(metadata_length_offset), &metadata_len, sizeof(uint32_t));

  ConstBuffer constbuffer(&buffer);
  auto&& [st_filter, filter1]{FilterCreate::deserialize(&constbuffer)};
  REQUIRE(st_filter.ok());

  // Check type
  CHECK(filter1.value()->type() == filtertype0);
}

TEST_CASE(
    "Filter: Test byte shuffle filter deserialization",
    "[filter][byte-shuffle]") {
  Buffer buffer;
  FilterType filtertype0 = FilterType::FILTER_BYTESHUFFLE;
  auto type = static_cast<uint8_t>(filtertype0);
  REQUIRE(buffer.write(&type, sizeof(uint8_t)).ok());

  // Save a pointer to store the actual length of the metadata.
  uint32_t metadata_len = 0;
  auto metadata_length_offset = buffer.offset();
  REQUIRE(buffer.write(&metadata_len, sizeof(uint32_t)).ok());

  // Filter-specific serialization
  uint64_t buff_size = buffer.size();

  metadata_len = static_cast<uint32_t>(buffer.size() - buff_size);
  std::memcpy(
      buffer.data(metadata_length_offset), &metadata_len, sizeof(uint32_t));

  ConstBuffer constbuffer(&buffer);
  auto&& [st_filter, filter1]{FilterCreate::deserialize(&constbuffer)};
  REQUIRE(st_filter.ok());

  // Check type
  CHECK(filter1.value()->type() == filtertype0);
}

TEST_CASE(
    "Filter: Test checksum md5 filter deserialization",
    "[filter][checksum-md5]") {
  Buffer buffer;
  FilterType filtertype0 = FilterType::FILTER_CHECKSUM_MD5;
  auto type = static_cast<uint8_t>(filtertype0);
  REQUIRE(buffer.write(&type, sizeof(uint8_t)).ok());

  // Save a pointer to store the actual length of the metadata.
  uint32_t metadata_len = 0;
  auto metadata_length_offset = buffer.offset();
  REQUIRE(buffer.write(&metadata_len, sizeof(uint32_t)).ok());

  // Filter-specific serialization
  uint64_t buff_size = buffer.size();

  metadata_len = static_cast<uint32_t>(buffer.size() - buff_size);
  std::memcpy(
      buffer.data(metadata_length_offset), &metadata_len, sizeof(uint32_t));
  ConstBuffer constbuffer(&buffer);
  auto&& [st_filter, filter1]{FilterCreate::deserialize(&constbuffer)};
  REQUIRE(st_filter.ok());

  // Check type
  CHECK(filter1.value()->type() == filtertype0);
}

TEST_CASE(
    "Filter: Test checksum sha256 filter deserialization",
    "[filter][checksum-sha256]") {
  Buffer buffer;
  FilterType filtertype0 = FilterType::FILTER_CHECKSUM_SHA256;
  auto type = static_cast<uint8_t>(filtertype0);
  REQUIRE(buffer.write(&type, sizeof(uint8_t)).ok());

  // Save a pointer to store the actual length of the metadata.
  uint32_t metadata_len = 0;
  auto metadata_length_offset = buffer.offset();
  REQUIRE(buffer.write(&metadata_len, sizeof(uint32_t)).ok());

  // Filter-specific serialization
  uint64_t buff_size = buffer.size();

  metadata_len = static_cast<uint32_t>(buffer.size() - buff_size);
  std::memcpy(
      buffer.data(metadata_length_offset), &metadata_len, sizeof(uint32_t));

  ConstBuffer constbuffer(&buffer);
  auto&& [st_filter, filter1]{FilterCreate::deserialize(&constbuffer)};
  REQUIRE(st_filter.ok());

  // Check type
  CHECK(filter1.value()->type() == filtertype0);
}

TEST_CASE(
    "Filter: Test encryption aes256gcm filter deserialization",
    "[filter][encryption-aes256gcm]") {
  Buffer buffer;
  FilterType filtertype0 = FilterType::INTERNAL_FILTER_AES_256_GCM;
  auto type = static_cast<uint8_t>(filtertype0);
  REQUIRE(buffer.write(&type, sizeof(uint8_t)).ok());

  // Save a pointer to store the actual length of the metadata.
  uint32_t metadata_len = 0;
  auto metadata_length_offset = buffer.offset();
  REQUIRE(buffer.write(&metadata_len, sizeof(uint32_t)).ok());

  // Filter-specific serialization
  uint64_t buff_size = buffer.size();

  metadata_len = static_cast<uint32_t>(buffer.size() - buff_size);
  std::memcpy(
      buffer.data(metadata_length_offset), &metadata_len, sizeof(uint32_t));

  ConstBuffer constbuffer(&buffer);
  auto&& [st_filter, filter1]{FilterCreate::deserialize(&constbuffer)};
  REQUIRE(st_filter.ok());

  // Check type
  CHECK(filter1.value()->type() == filtertype0);
}

TEST_CASE(
    "Filter: Test compression filter deserialization",
    "[filter][compression]") {
  SECTION("no level compression") {
    auto filtertype0 =
        GENERATE(FilterType::FILTER_RLE, FilterType::FILTER_DOUBLE_DELTA);
    Compressor compressor0 = Compressor::NO_COMPRESSION;
    switch (filtertype0) {
      case FilterType::FILTER_RLE:
        compressor0 = Compressor::RLE;
        break;
      case FilterType::FILTER_DOUBLE_DELTA:
        compressor0 = Compressor::DOUBLE_DELTA;
        break;
      default:
        compressor0 = Compressor::NO_COMPRESSION;
        break;
    }
    int level0 = 0;

    Buffer buffer;

    auto type = static_cast<uint8_t>(filtertype0);
    REQUIRE(buffer.write(&type, sizeof(uint8_t)).ok());

    // Save a pointer to store the actual length of the metadata.
    uint32_t metadata_len = 0;
    auto metadata_length_offset = buffer.offset();
    REQUIRE(buffer.write(&metadata_len, sizeof(uint32_t)).ok());

    // Filter-specific serialization
    uint64_t buff_size = buffer.size();

    auto compressor_char = static_cast<uint8_t>(compressor0);
    REQUIRE(buffer.write(&compressor_char, sizeof(uint8_t)).ok());
    REQUIRE(buffer.write(&level0, sizeof(int32_t)).ok());

    metadata_len = static_cast<uint32_t>(buffer.size() - buff_size);
    std::memcpy(
        buffer.data(metadata_length_offset), &metadata_len, sizeof(uint32_t));

    ConstBuffer constbuffer(&buffer);
    auto&& [st_filter, filter1]{FilterCreate::deserialize(&constbuffer)};
    REQUIRE(st_filter.ok());

    // Check type
    CHECK(filter1.value()->type() == filtertype0);
  }

  SECTION("gzip") {
    auto level0 = GENERATE(1, 2, 3, 4, 5, 6, 7, 8, 9);
    Compressor compressor0 = Compressor::GZIP;
    FilterType filtertype0 = FilterType::FILTER_GZIP;

    Buffer buffer;

    auto type = static_cast<uint8_t>(filtertype0);
    REQUIRE(buffer.write(&type, sizeof(uint8_t)).ok());

    // Save a pointer to store the actual length of the metadata.
    uint32_t metadata_len = 0;
    auto metadata_length_offset = buffer.offset();
    REQUIRE(buffer.write(&metadata_len, sizeof(uint32_t)).ok());

    // Filter-specific serialization
    uint64_t buff_size = buffer.size();

    auto compressor_char = static_cast<uint8_t>(compressor0);
    REQUIRE(buffer.write(&compressor_char, sizeof(uint8_t)).ok());
    REQUIRE(buffer.write(&level0, sizeof(int32_t)).ok());

    metadata_len = static_cast<uint32_t>(buffer.size() - buff_size);
    std::memcpy(
        buffer.data(metadata_length_offset), &metadata_len, sizeof(uint32_t));

    ConstBuffer constbuffer(&buffer);
    auto&& [st_filter, filter1]{FilterCreate::deserialize(&constbuffer)};
    REQUIRE(st_filter.ok());

    // Check type
    CHECK(filter1.value()->type() == filtertype0);

    int compressionlevel1 = 0;
    REQUIRE(
        filter1.value()
            ->get_option(FilterOption::COMPRESSION_LEVEL, &compressionlevel1)
            .ok());
    CHECK(level0 == compressionlevel1);
  }

  SECTION("zstd") {
    // zstd levels range from -7(fastest) to 22
    auto level0 = GENERATE(-7, -5, -3, 3, 5, 7, 9, 15, 22);
    Compressor compressor0 = Compressor::ZSTD;
    FilterType filtertype0 = FilterType::FILTER_ZSTD;

    Buffer buffer;

    auto type = static_cast<uint8_t>(filtertype0);
    REQUIRE(buffer.write(&type, sizeof(uint8_t)).ok());

    // Save a pointer to store the actual length of the metadata.
    uint32_t metadata_len = 0;
    auto metadata_length_offset = buffer.offset();
    REQUIRE(buffer.write(&metadata_len, sizeof(uint32_t)).ok());

    // Filter-specific serialization
    uint64_t buff_size = buffer.size();

    auto compressor_char = static_cast<uint8_t>(compressor0);
    REQUIRE(buffer.write(&compressor_char, sizeof(uint8_t)).ok());
    REQUIRE(buffer.write(&level0, sizeof(int32_t)).ok());

    metadata_len = static_cast<uint32_t>(buffer.size() - buff_size);
    std::memcpy(
        buffer.data(metadata_length_offset), &metadata_len, sizeof(uint32_t));

    ConstBuffer constbuffer(&buffer);
    auto&& [st_filter, filter1]{FilterCreate::deserialize(&constbuffer)};
    REQUIRE(st_filter.ok());

    // Check type
    CHECK(filter1.value()->type() == filtertype0);

    int compressionlevel1 = 0;
    REQUIRE(
        filter1.value()
            ->get_option(FilterOption::COMPRESSION_LEVEL, &compressionlevel1)
            .ok());
    CHECK(level0 == compressionlevel1);
  }

  SECTION("lz4") {
    // lz4 levels range from 1 to 12
    auto level0 = GENERATE(1, 2, 3, 5, 7, 8, 9, 11, 12);
    Compressor compressor0 = Compressor::LZ4;
    FilterType filtertype0 = FilterType::FILTER_LZ4;

    Buffer buffer;

    auto type = static_cast<uint8_t>(filtertype0);
    REQUIRE(buffer.write(&type, sizeof(uint8_t)).ok());

    // Save a pointer to store the actual length of the metadata.
    uint32_t metadata_len = 0;
    auto metadata_length_offset = buffer.offset();
    REQUIRE(buffer.write(&metadata_len, sizeof(uint32_t)).ok());

    // Filter-specific serialization
    uint64_t buff_size = buffer.size();

    auto compressor_char = static_cast<uint8_t>(compressor0);
    REQUIRE(buffer.write(&compressor_char, sizeof(uint8_t)).ok());
    REQUIRE(buffer.write(&level0, sizeof(int32_t)).ok());

    metadata_len = static_cast<uint32_t>(buffer.size() - buff_size);
    std::memcpy(
        buffer.data(metadata_length_offset), &metadata_len, sizeof(uint32_t));

    ConstBuffer constbuffer(&buffer);
    auto&& [st_filter, filter1]{FilterCreate::deserialize(&constbuffer)};
    REQUIRE(st_filter.ok());

    // Check type
    CHECK(filter1.value()->type() == filtertype0);

    int compressionlevel1 = 0;
    REQUIRE(
        filter1.value()
            ->get_option(FilterOption::COMPRESSION_LEVEL, &compressionlevel1)
            .ok());
    CHECK(level0 == compressionlevel1);
  }

  SECTION("bzip2") {
    // bzip2 levels range from 1 to 9
    auto level0 = GENERATE(1, 2, 3, 4, 5, 6, 7, 8, 9);
    Compressor compressor0 = Compressor::BZIP2;
    FilterType filtertype0 = FilterType::FILTER_BZIP2;

    Buffer buffer;

    auto type = static_cast<uint8_t>(filtertype0);
    REQUIRE(buffer.write(&type, sizeof(uint8_t)).ok());

    // Save a pointer to store the actual length of the metadata.
    uint32_t metadata_len = 0;
    auto metadata_length_offset = buffer.offset();
    REQUIRE(buffer.write(&metadata_len, sizeof(uint32_t)).ok());

    // Filter-specific serialization
    uint64_t buff_size = buffer.size();

    auto compressor_char = static_cast<uint8_t>(compressor0);
    REQUIRE(buffer.write(&compressor_char, sizeof(uint8_t)).ok());
    REQUIRE(buffer.write(&level0, sizeof(int32_t)).ok());

    metadata_len = static_cast<uint32_t>(buffer.size() - buff_size);
    std::memcpy(
        buffer.data(metadata_length_offset), &metadata_len, sizeof(uint32_t));

    ConstBuffer constbuffer(&buffer);
    auto&& [st_filter, filter1]{FilterCreate::deserialize(&constbuffer)};
    REQUIRE(st_filter.ok());

    // Check type
    CHECK(filter1.value()->type() == filtertype0);

    int compressionlevel1 = 0;
    REQUIRE(
        filter1.value()
            ->get_option(FilterOption::COMPRESSION_LEVEL, &compressionlevel1)
            .ok());
    CHECK(level0 == compressionlevel1);
  }
}

TEST_CASE("Filter: Test noop filter deserialization", "[filter][noop]") {
  Buffer buffer;
  FilterType filtertype0 = FilterType::FILTER_NONE;
  auto type = static_cast<uint8_t>(filtertype0);
  REQUIRE(buffer.write(&type, sizeof(uint8_t)).ok());

  // Save a pointer to store the actual length of the metadata.
  uint32_t metadata_len = 0;
  auto metadata_length_offset = buffer.offset();
  REQUIRE(buffer.write(&metadata_len, sizeof(uint32_t)).ok());

  // Filter-specific serialization
  uint64_t buff_size = buffer.size();

  metadata_len = static_cast<uint32_t>(buffer.size() - buff_size);
  std::memcpy(
      buffer.data(metadata_length_offset), &metadata_len, sizeof(uint32_t));

  ConstBuffer constbuffer(&buffer);
  auto&& [st_filter, filter1]{FilterCreate::deserialize(&constbuffer)};
  REQUIRE(st_filter.ok());

  // Check type
  CHECK(filter1.value()->type() == filtertype0);
}

TEST_CASE(
    "Filter: Test positive delta filter deserialization",
    "[filter][positive-delta]") {
  Buffer buffer;
  FilterType filtertype0 = FilterType::FILTER_POSITIVE_DELTA;
  auto type = static_cast<uint8_t>(filtertype0);
  REQUIRE(buffer.write(&type, sizeof(uint8_t)).ok());

  // Save a pointer to store the actual length of the metadata.
  uint32_t metadata_len = 0;
  auto metadata_length_offset = buffer.offset();
  REQUIRE(buffer.write(&metadata_len, sizeof(uint32_t)).ok());

  // Filter-specific serialization
  uint64_t buff_size = buffer.size();

  uint32_t max_window_size0 = 1024;
  REQUIRE((buffer.write(&max_window_size0, sizeof(uint32_t))).ok());

  metadata_len = static_cast<uint32_t>(buffer.size() - buff_size);
  std::memcpy(
      buffer.data(metadata_length_offset), &metadata_len, sizeof(uint32_t));

  ConstBuffer constbuffer(&buffer);
  auto&& [st_filter, filter1]{FilterCreate::deserialize(&constbuffer)};
  REQUIRE(st_filter.ok());

  // Check type
  CHECK(filter1.value()->type() == filtertype0);

  uint32_t max_window_size1 = 0;
  REQUIRE(filter1.value()
              ->get_option(
                  FilterOption::POSITIVE_DELTA_MAX_WINDOW, &max_window_size1)
              .ok());
  CHECK(max_window_size0 == max_window_size1);
}