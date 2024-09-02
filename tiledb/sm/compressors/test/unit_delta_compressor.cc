/**
 * @file   unit_delta_compressor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB Inc.
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
 * Tests for the dictionary encoding of strings.
 */

#include <test/support/tdb_catch.h>

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/compressors/delta_compressor.h"
#include "tiledb/sm/enums/datatype.h"

#include <cstring>
#include <iterator>

using namespace tiledb::common;
using namespace tiledb::sm;

TEST_CASE("Test delta compression of a vector", "[compression][delta]") {
  std::vector<int64_t> uncompressed{0, 1, 1, 15, 3, 0, 2, 7, 1};
  std::vector<int64_t> expected{0, 1, 0, 14, -12, -3, 2, 5, -6};

  tiledb::sm::ConstBuffer uncompressed_buff(
      uncompressed.data(),
      uncompressed.size() * sizeof(decltype(uncompressed)::value_type));
  tiledb::sm::Buffer compressed_buff{};

  REQUIRE_NOTHROW(tiledb::sm::Delta::compress(
      Datatype::INT64, &uncompressed_buff, &compressed_buff));

  std::vector<int64_t> compressed(expected.size());
  compressed.assign(
      reinterpret_cast<int64_t*>((char*)compressed_buff.data() + 8),
      reinterpret_cast<int64_t*>(compressed_buff.data()) +
          (compressed_buff.size() / sizeof(decltype(compressed)::value_type)));
  CHECK(compressed == expected);
}

TEST_CASE("Test delta decompression of a vector", "[decompression][delta]") {
  // NOTE: first two values are [bitsize, num_values]
  std::vector<int64_t> compressed_data{0, 1, 0, 14, -12, -3, 2, 5, -6};
  std::vector<std::byte> compressed_raw(
      8 +
      compressed_data.size() * sizeof(decltype(compressed_data)::value_type));

  ((uint64_t*)(compressed_raw.data()))[0] = compressed_data.size();
  size_t uncompressed_bytes =
      compressed_data.size() * sizeof(decltype(compressed_data)::value_type);

  std::memcpy(
      (std::byte*)compressed_raw.data() + 8,
      compressed_data.data(),
      uncompressed_bytes);

  tiledb::sm::ConstBuffer compressed_buff(
      compressed_raw.data(), compressed_raw.size());

  Buffer uncompressed_rawbuf(uncompressed_bytes);
  tiledb::sm::PreallocatedBuffer uncompressed_buff{
      uncompressed_rawbuf.data(), uncompressed_bytes};

  REQUIRE_NOTHROW(tiledb::sm::Delta::decompress(
      Datatype::INT64, &compressed_buff, &uncompressed_buff));

  std::vector<int64_t> uncompressed(compressed_data.size());
  uncompressed.assign(
      reinterpret_cast<const int64_t*>(uncompressed_buff.data()),
      reinterpret_cast<const int64_t*>(uncompressed_buff.data()) +
          uncompressed_buff.size() /
              sizeof(decltype(compressed_data)::value_type));

  std::vector<int64_t> expected{0, 1, 1, 15, 3, 0, 2, 7, 1};
  CHECK(uncompressed == expected);
}
