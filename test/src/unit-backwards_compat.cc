/**
 * @file   unit-backwards_compat.cc
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
 * Tests of backwards compatibility for opening/reading arrays.
 */

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"

#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>

using namespace tiledb;

static const std::string arrays_dir =
    std::string(TILEDB_TEST_INPUTS_DIR) + "/arrays";

TEST_CASE(
    "Backwards compatibility: Test error opening 1.3.0 array",
    "[backwards-compat]") {
  Context ctx;
  std::string array_uri(arrays_dir + "/dense_array_v1_3_0");
  try {
    Array array(ctx, array_uri, TILEDB_READ);
    REQUIRE(false);
  } catch (const TileDBError& e) {
    // Check correct exception type and error message.
    std::string msg(e.what());
    REQUIRE(msg.find("Error: Read buffer overflow") != std::string::npos);
  } catch (const std::exception& e) {
    REQUIRE(false);
  }
}