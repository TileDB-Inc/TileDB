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
 * Tests the `FilterBuffer` class.
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
#include "tiledb/sm/filter/noop_filter.h"
#include "tiledb/sm/filter/positive_delta_filter.h"

#include <catch.hpp>
#include <iostream>

using namespace tiledb::sm;

TEST_CASE(
    "Filter: Test bit width reduction filter deserialization",
    "[filter][bit-width-reduction]") {
  uint32_t max_window_size0 = 1024;
  BitWidthReductionFilter filter0(max_window_size0);
  Buffer buffer;
  filter0.serialize(&buffer);

  ConstBuffer constbuffer(&buffer);
  auto&& [st_filter, filter1]{Filter::deserialize(&constbuffer)};
  CHECK(st_filter.ok());

  // Check type
  CHECK(filter0.type() == filter1.value()->type());

  uint32_t max_window_size1 = 0;
  REQUIRE(
      filter1.value()
          ->get_option(FilterOption::BIT_WIDTH_MAX_WINDOW, &max_window_size1)
          .ok());
  CHECK(max_window_size0 == max_window_size1);
  buffer.clear();
}

TEST_CASE(
    "Filter: Test bit shuffle filter deserialization",
    "[filter][bit-shuffle]") {
  BitshuffleFilter filter0;
  Buffer buffer;
  filter0.serialize(&buffer);

  ConstBuffer constbuffer(&buffer);
  auto&& [st_filter, filter1]{Filter::deserialize(&constbuffer)};
  CHECK(st_filter.ok());

  // Check type
  CHECK(filter0.type() == filter1.value()->type());
  buffer.clear();
}

TEST_CASE(
    "Filter: Test byte shuffle filter deserialization",
    "[filter][byte-shuffle]") {
  ByteshuffleFilter filter0;
  Buffer buffer;
  filter0.serialize(&buffer);

  ConstBuffer constbuffer(&buffer);
  auto&& [st_filter, filter1]{Filter::deserialize(&constbuffer)};
  CHECK(st_filter.ok());

  // Check type
  CHECK(filter0.type() == filter1.value()->type());
  buffer.clear();
}

TEST_CASE(
    "Filter: Test checksum md5 filter deserialization",
    "[filter][checksum-md5]") {
  ChecksumMD5Filter filter0;
  Buffer buffer;
  filter0.serialize(&buffer);

  ConstBuffer constbuffer(&buffer);
  auto&& [st_filter, filter1]{Filter::deserialize(&constbuffer)};
  CHECK(st_filter.ok());

  // Check type
  CHECK(filter0.type() == filter1.value()->type());
  buffer.clear();
}

TEST_CASE(
    "Filter: Test checksum sha256 filter deserialization",
    "[filter][checksum-sha256]") {
  ChecksumSHA256Filter filter0;
  Buffer buffer;
  filter0.serialize(&buffer);

  ConstBuffer constbuffer(&buffer);
  auto&& [st_filter, filter1]{Filter::deserialize(&constbuffer)};
  CHECK(st_filter.ok());

  // Check type
  CHECK(filter0.type() == filter1.value()->type());
  buffer.clear();
}

TEST_CASE(
    "Filter: Test encryption aes256gcm filter deserialization",
    "[filter][encryption-aes256gcm]") {
  EncryptionKey encryptionkey0;
  encryptionkey0.set_key(
      EncryptionType::AES_256_GCM, "abcdefghijklmnopqrstuvwxyz012345", 32);
  EncryptionAES256GCMFilter filter0(encryptionkey0);

  Buffer buffer;
  filter0.serialize(&buffer);

  ConstBuffer constbuffer(&buffer);
  auto&& [st_filter, filter1]{Filter::deserialize(&constbuffer)};
  CHECK(st_filter.ok());

  // Check type
  CHECK(filter0.type() == filter1.value()->type());
  buffer.clear();
}

TEST_CASE(
    "Filter: Test compression filter deserialization",
    "[filter][compression]") {
  SECTION("no level compression") {
    auto compressortype0 = GENERATE(
        Compressor::NO_COMPRESSION, Compressor::RLE, Compressor::DOUBLE_DELTA);
    int compressionlevel0 = 0;
    CompressionFilter filter0(compressortype0, compressionlevel0);
    Buffer buffer;
    filter0.serialize(&buffer);

    ConstBuffer constbuffer(&buffer);
    auto&& [st_filter, filter1]{Filter::deserialize(&constbuffer)};
    CHECK(st_filter.ok());

    // Check type
    CHECK(filter0.type() == filter1.value()->type());
    buffer.clear();
  }

  SECTION("gzip") {
    Compressor compressortype0 = Compressor::GZIP;
    auto compressionlevel0 = GENERATE(1, 2, 3, 4, 5, 6, 7, 8, 9);
    CompressionFilter filter0(compressortype0, compressionlevel0);
    Buffer buffer;
    filter0.serialize(&buffer);

    ConstBuffer constbuffer(&buffer);
    auto&& [st_filter, filter1]{Filter::deserialize(&constbuffer)};
    CHECK(st_filter.ok());

    // Check type
    CHECK(filter0.type() == filter1.value()->type());

    int compressionlevel1 = 0;
    REQUIRE(
        filter1.value()
            ->get_option(FilterOption::COMPRESSION_LEVEL, &compressionlevel1)
            .ok());
    CHECK(compressionlevel0 == compressionlevel1);
    buffer.clear();
  }

  SECTION("zstd") {
    Compressor compressortype0 = Compressor::ZSTD;
    // zstd levels range from -7(fastest) to 22
    auto compressionlevel0 = GENERATE(-7, -5, -3, 3, 5, 7, 9, 15, 22);
    CompressionFilter filter0(compressortype0, compressionlevel0);
    Buffer buffer;
    filter0.serialize(&buffer);

    ConstBuffer constbuffer(&buffer);
    auto&& [st_filter, filter1]{Filter::deserialize(&constbuffer)};
    CHECK(st_filter.ok());

    // Check type
    CHECK(filter0.type() == filter1.value()->type());

    int compressionlevel1 = 0;
    REQUIRE(
        filter1.value()
            ->get_option(FilterOption::COMPRESSION_LEVEL, &compressionlevel1)
            .ok());
    CHECK(compressionlevel0 == compressionlevel1);
    buffer.clear();
  }

  SECTION("lz4") {
    Compressor compressortype0 = Compressor::LZ4;
    // lz4 levels range from 1 to 12
    auto compressionlevel0 = GENERATE(1, 2, 3, 5, 7, 8, 9, 11, 12);
    CompressionFilter filter0(compressortype0, compressionlevel0);
    Buffer buffer;
    filter0.serialize(&buffer);

    ConstBuffer constbuffer(&buffer);
    auto&& [st_filter, filter1]{Filter::deserialize(&constbuffer)};
    CHECK(st_filter.ok());

    // Check type
    CHECK(filter0.type() == filter1.value()->type());

    int compressionlevel1 = 0;
    REQUIRE(
        filter1.value()
            ->get_option(FilterOption::COMPRESSION_LEVEL, &compressionlevel1)
            .ok());
    CHECK(compressionlevel0 == compressionlevel1);
    buffer.clear();
  }

  SECTION("bzip2") {
    Compressor compressortype0 = Compressor::BZIP2;
    // bzip2 levels range from 1 to 9
    auto compressionlevel0 = GENERATE(1, 2, 3, 4, 5, 6, 7, 8, 9);
    CompressionFilter filter0(compressortype0, compressionlevel0);
    Buffer buffer;
    filter0.serialize(&buffer);

    ConstBuffer constbuffer(&buffer);
    auto&& [st_filter, filter1]{Filter::deserialize(&constbuffer)};
    CHECK(st_filter.ok());

    // Check type
    CHECK(filter0.type() == filter1.value()->type());

    int compressionlevel1 = 0;
    REQUIRE(
        filter1.value()
            ->get_option(FilterOption::COMPRESSION_LEVEL, &compressionlevel1)
            .ok());
    CHECK(compressionlevel0 == compressionlevel1);
    buffer.clear();
  }
}

TEST_CASE("Filter: Test noop filter deserialization", "[filter][noop]") {
  NoopFilter filter0;
  Buffer buffer;
  filter0.serialize(&buffer);

  ConstBuffer constbuffer(&buffer);
  auto&& [st_filter, filter1]{Filter::deserialize(&constbuffer)};
  CHECK(st_filter.ok());

  // Check type
  CHECK(filter0.type() == filter1.value()->type());
}

TEST_CASE(
    "Filter: Test positive delta filter deserialization",
    "[filter][positive-delta]") {
  uint32_t max_window_size0 = 1024;
  PositiveDeltaFilter filter0(max_window_size0);
  Buffer buffer;
  filter0.serialize(&buffer);

  ConstBuffer constbuffer(&buffer);
  auto&& [st_filter, filter1]{Filter::deserialize(&constbuffer)};
  CHECK(st_filter.ok());

  // Check type
  CHECK(filter0.type() == filter1.value()->type());

  uint32_t max_window_size1 = 0;
  REQUIRE(filter1.value()
              ->get_option(
                  FilterOption::POSITIVE_DELTA_MAX_WINDOW, &max_window_size1)
              .ok());
  CHECK(max_window_size0 == max_window_size1);
  buffer.clear();
}