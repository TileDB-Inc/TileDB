/**
 * @file   unit-status.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * Tests the `Status` class.
 */

#include <catch.hpp>
#include "tiledb/sm/misc/status.h"

using namespace tiledb::sm;

TEST_CASE("Status: Test ok", "[status]") {
  Status st = Status::Ok();
  CHECK(st.ok());
  st = Status::Error("err msg");
  CHECK(!st.ok());
}

TEST_CASE("Status: Test code and message", "[status]") {
  Status ok = Status::Ok();
  CHECK(StatusCode::Ok == ok.code());

  Status err = Status::Error("err msg");
  CHECK(StatusCode::Error == err.code());
  CHECK_THAT(err.message(), Catch::Equals("err msg"));
}

TEST_CASE("Status: Test to_string", "[status]") {
  Status ok = Status::Ok();
  CHECK_THAT(ok.to_string(), Catch::Equals("Ok"));

  Status err = Status::Error("err msg");
  CHECK_THAT(err.to_string(), Catch::Equals("Error: err msg"));
}

TEST_CASE("Status: Test code_to_string", "[status]") {
  Status ok = Status::Ok();
  CHECK_THAT(ok.code_to_string(), Catch::Equals("Ok"));

  Status err = Status::Error("err message");
  CHECK_THAT(err.code_to_string(), Catch::Equals("Error"));
}

TEST_CASE("Status: Test posix_code", "[status]") {
  Status st = Status::Ok();
  // check that posix code is < 0 by default
  CHECK(st.posix_code() == -1);

  st = Status::Error("err msg");
  CHECK(st.posix_code() == -1);
}
