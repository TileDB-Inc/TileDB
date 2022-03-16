/**
 * @file tiledb/sm/filter/test/unit_filter_create.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * This file tests FilterCreate class
 *
 */

#include <catch.hpp>
#include "../filter_create.h"

#include "../bit_width_reduction_filter.h"
#include "../bitshuffle_filter.h"
#include "../byteshuffle_filter.h"
#include "../checksum_md5_filter.h"
#include "../checksum_sha256_filter.h"
#include "../compression_filter.h"
#include "../encryption_aes256gcm_filter.h"
#include "../filter.h"
#include "../noop_filter.h"
#include "../positive_delta_filter.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"

using namespace tiledb::sm;

template <class T, int n>
inline T& buffer_offset(void* p) {
  return *static_cast<T*>(static_cast<void*>(static_cast<char*>(p) + n));
}

TEST_CASE(
    "Filter: Test bit width reduction filter deserialization",
    "[filter][bit-width-reduction]") {
  FilterType filtertype0 = FilterType::FILTER_BIT_WIDTH_REDUCTION;
  uint32_t max_window_size0 = 1024;
  char serialized_buffer[9];
  char* p = &serialized_buffer[0];
  buffer_offset<uint8_t, 0>(p) = static_cast<uint8_t>(filtertype0);
  buffer_offset<uint32_t, 1>(p) = sizeof(uint32_t);  // metadata_length
  buffer_offset<uint32_t, 5>(p) = max_window_size0;

  ConstBuffer constbuffer(&serialized_buffer, sizeof(serialized_buffer));
  auto&& [st_filter, filter1]{
      FilterCreate::deserialize(&constbuffer, constants::format_version)};
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
  FilterType filtertype0 = FilterType::FILTER_BITSHUFFLE;
  char serialized_buffer[5];
  char* p = &serialized_buffer[0];
  buffer_offset<uint8_t, 0>(p) = static_cast<uint8_t>(filtertype0);
  buffer_offset<uint32_t, 1>(p) = 0;  // metadata_length

  ConstBuffer constbuffer(&serialized_buffer, sizeof(serialized_buffer));
  auto&& [st_filter, filter1]{
      FilterCreate::deserialize(&constbuffer, constants::format_version)};
  REQUIRE(st_filter.ok());

  // Check type
  CHECK(filter1.value()->type() == filtertype0);
}

TEST_CASE(
    "Filter: Test byte shuffle filter deserialization",
    "[filter][byte-shuffle]") {
  FilterType filtertype0 = FilterType::FILTER_BYTESHUFFLE;
  char serialized_buffer[5];
  char* p = &serialized_buffer[0];
  buffer_offset<uint8_t, 0>(p) = static_cast<uint8_t>(filtertype0);
  buffer_offset<uint32_t, 1>(p) = 0;  // metadata_length

  ConstBuffer constbuffer(&serialized_buffer, sizeof(serialized_buffer));
  auto&& [st_filter, filter1]{
      FilterCreate::deserialize(&constbuffer, constants::format_version)};
  REQUIRE(st_filter.ok());

  // Check type
  CHECK(filter1.value()->type() == filtertype0);
}

TEST_CASE(
    "Filter: Test checksum md5 filter deserialization",
    "[filter][checksum-md5]") {
  FilterType filtertype0 = FilterType::FILTER_CHECKSUM_MD5;
  char serialized_buffer[5];
  char* p = &serialized_buffer[0];
  buffer_offset<uint8_t, 0>(p) = static_cast<uint8_t>(filtertype0);
  buffer_offset<uint32_t, 1>(p) = 0;  // metadata_length

  ConstBuffer constbuffer(&serialized_buffer, sizeof(serialized_buffer));
  auto&& [st_filter, filter1]{
      FilterCreate::deserialize(&constbuffer, constants::format_version)};
  REQUIRE(st_filter.ok());

  // Check type
  CHECK(filter1.value()->type() == filtertype0);
}

TEST_CASE(
    "Filter: Test checksum sha256 filter deserialization",
    "[filter][checksum-sha256]") {
  FilterType filtertype0 = FilterType::FILTER_CHECKSUM_SHA256;
  char serialized_buffer[5];
  char* p = &serialized_buffer[0];
  buffer_offset<uint8_t, 0>(p) = static_cast<uint8_t>(filtertype0);
  buffer_offset<uint32_t, 1>(p) = 0;  // metadata_length

  ConstBuffer constbuffer(&serialized_buffer, sizeof(serialized_buffer));
  auto&& [st_filter, filter1]{
      FilterCreate::deserialize(&constbuffer, constants::format_version)};
  REQUIRE(st_filter.ok());

  // Check type
  CHECK(filter1.value()->type() == filtertype0);
}

TEST_CASE(
    "Filter: Test encryption aes256gcm filter deserialization",
    "[filter][encryption-aes256gcm]") {
  FilterType filtertype0 = FilterType::INTERNAL_FILTER_AES_256_GCM;
  char serialized_buffer[5];
  char* p = &serialized_buffer[0];
  buffer_offset<uint8_t, 0>(p) = static_cast<uint8_t>(filtertype0);
  buffer_offset<uint32_t, 1>(p) = 0;  // metadata_length

  ConstBuffer constbuffer(&serialized_buffer, sizeof(serialized_buffer));
  auto&& [st_filter, filter1]{
      FilterCreate::deserialize(&constbuffer, constants::format_version)};
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

    char serialized_buffer[10];
    char* p = &serialized_buffer[0];
    buffer_offset<uint8_t, 0>(p) = static_cast<uint8_t>(filtertype0);
    buffer_offset<uint32_t, 1>(p) =
        sizeof(uint8_t) + sizeof(int32_t);  // metadata_length
    buffer_offset<uint8_t, 5>(p) = static_cast<uint8_t>(compressor0);

    ConstBuffer constbuffer(&serialized_buffer, sizeof(serialized_buffer));
    auto&& [st_filter, filter1]{
        FilterCreate::deserialize(&constbuffer, constants::format_version)};
    REQUIRE(st_filter.ok());

    // Check type
    CHECK(filter1.value()->type() == filtertype0);
  }

  SECTION("gzip") {
    auto level0 = GENERATE(1, 2, 3, 4, 5, 6, 7, 8, 9);
    Compressor compressor0 = Compressor::GZIP;
    FilterType filtertype0 = FilterType::FILTER_GZIP;

    char serialized_buffer[10];
    char* p = &serialized_buffer[0];
    buffer_offset<uint8_t, 0>(p) = static_cast<uint8_t>(filtertype0);
    buffer_offset<uint32_t, 1>(p) =
        sizeof(uint8_t) + sizeof(int32_t);  // metadata_length
    buffer_offset<uint8_t, 5>(p) = static_cast<uint8_t>(compressor0);
    buffer_offset<int32_t, 6>(p) = level0;

    ConstBuffer constbuffer(&serialized_buffer, sizeof(serialized_buffer));
    auto&& [st_filter, filter1]{
        FilterCreate::deserialize(&constbuffer, constants::format_version)};
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

    char serialized_buffer[10];
    char* p = &serialized_buffer[0];
    buffer_offset<uint8_t, 0>(p) = static_cast<uint8_t>(filtertype0);
    buffer_offset<uint32_t, 1>(p) =
        sizeof(uint8_t) + sizeof(int32_t);  // metadata_length
    buffer_offset<uint8_t, 5>(p) = static_cast<uint8_t>(compressor0);
    buffer_offset<int32_t, 6>(p) = level0;

    ConstBuffer constbuffer(&serialized_buffer, sizeof(serialized_buffer));
    auto&& [st_filter, filter1]{
        FilterCreate::deserialize(&constbuffer, constants::format_version)};
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

    char serialized_buffer[10];
    char* p = &serialized_buffer[0];
    buffer_offset<uint8_t, 0>(p) = static_cast<uint8_t>(filtertype0);
    buffer_offset<uint32_t, 1>(p) =
        sizeof(uint8_t) + sizeof(int32_t);  // metadata_length
    buffer_offset<uint8_t, 5>(p) = static_cast<uint8_t>(compressor0);
    buffer_offset<int32_t, 6>(p) = level0;

    ConstBuffer constbuffer(&serialized_buffer, sizeof(serialized_buffer));
    auto&& [st_filter, filter1]{
        FilterCreate::deserialize(&constbuffer, constants::format_version)};
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

    char serialized_buffer[10];
    char* p = &serialized_buffer[0];
    buffer_offset<uint8_t, 0>(p) = static_cast<uint8_t>(filtertype0);
    buffer_offset<uint32_t, 1>(p) =
        sizeof(uint8_t) + sizeof(int32_t);  // metadata_length
    buffer_offset<uint8_t, 5>(p) = static_cast<uint8_t>(compressor0);
    buffer_offset<int32_t, 6>(p) = level0;

    ConstBuffer constbuffer(&serialized_buffer, sizeof(serialized_buffer));
    auto&& [st_filter, filter1]{
        FilterCreate::deserialize(&constbuffer, constants::format_version)};
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
  char serialized_buffer[5];
  char* p = &serialized_buffer[0];
  buffer_offset<uint8_t, 0>(p) = static_cast<uint8_t>(filtertype0);
  buffer_offset<uint32_t, 1>(p) = 0;  // metadata_length

  ConstBuffer constbuffer(&serialized_buffer, sizeof(serialized_buffer));
  auto&& [st_filter, filter1]{
      FilterCreate::deserialize(&constbuffer, constants::format_version)};
  REQUIRE(st_filter.ok());

  // Check type
  CHECK(filter1.value()->type() == filtertype0);
}

TEST_CASE(
    "Filter: Test positive delta filter deserialization",
    "[filter][positive-delta]") {
  FilterType filtertype0 = FilterType::FILTER_POSITIVE_DELTA;
  uint32_t max_window_size0 = 1024;
  char serialized_buffer[9];
  char* p = &serialized_buffer[0];
  buffer_offset<uint8_t, 0>(p) = static_cast<uint8_t>(filtertype0);
  buffer_offset<uint32_t, 1>(p) = sizeof(uint32_t);  // metadata_length
  buffer_offset<uint32_t, 5>(p) = max_window_size0;
  ConstBuffer constbuffer(&serialized_buffer, sizeof(serialized_buffer));
  auto&& [st_filter, filter1]{
      FilterCreate::deserialize(&constbuffer, constants::format_version)};
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