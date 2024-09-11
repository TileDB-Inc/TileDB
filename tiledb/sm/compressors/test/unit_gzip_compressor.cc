/**
 * @file   unit_gzip_compressor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB Inc.
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
 * Tests for the GZip compressor.
 */

#include "../gzip_compressor.h"
#include "tiledb/sm/buffer/buffer.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <cstring>
#include <iterator>
#include <sstream>

using namespace tiledb::common;
using namespace tiledb::sm;

TEST_CASE(
    "Compression-GZip: Test compression with too small output buffer fails",
    "[compression][gzip]") {
  auto size = GENERATE(0, 64, 1024);
  std::string str(size, 'a');
  ConstBuffer in_buf{str.data(), str.size()};

  Buffer out_buf;
  throw_if_not_ok(out_buf.realloc(1));

  CHECK_THROWS(GZip::compress(&in_buf, &out_buf));
}

TEST_CASE(
    "Compression-GZip: Test compression of empty buffer",
    "[compression][gzip]") {
  ConstBuffer in_buf{"", 0};

  Buffer out_buf;
  throw_if_not_ok(out_buf.realloc(GZip::overhead(in_buf.size())));

  GZip::compress(&in_buf, &out_buf);

  ConstBuffer in_buf_dec{out_buf.data(), out_buf.size()};
  std::vector<char> out_buf_dec_storage(1024);
  PreallocatedBuffer out_buf_dec{
      out_buf_dec_storage.data(), out_buf_dec_storage.size()};

  GZip::decompress(&in_buf_dec, &out_buf_dec);
  // Check that the empty buffer is compressed and decompressed correctly.
  CHECK(out_buf_dec.size() - out_buf_dec.free_space() == in_buf.size());
}
