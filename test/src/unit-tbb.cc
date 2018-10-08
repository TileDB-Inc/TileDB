/**
 * @file   unit-tbb.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * Tests for TileDB TBB thread runtime support
 */

#ifdef HAVE_TBB

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"

TEST_CASE("TileDB TBB negative number of set threads", "[tbb]") {
  tiledb::Config config;
  config["sm.num_tbb_threads"] = "-3";
  CHECK_THROWS(tiledb::Context(config));
}

TEST_CASE("TileDB TBB different number of set threads per process", "[tbb]") {
  // default number of TBB threads
  auto ctx1 = tiledb::Context();

  tiledb::Config config;
  config["sm.num_tbb_threads"] = "1000";
  CHECK_THROWS(tiledb::Context(config));
}

#endif
