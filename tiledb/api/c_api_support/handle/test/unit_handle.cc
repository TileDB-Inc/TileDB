/**
 * @file tiledb/api/proxy/test/unit_handle.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 */

#include <test/support/tdb_catch.h>
#include "../handle.h"

using namespace tiledb::api;

class TestHandle : public CAPIHandle {
 public:
  static constexpr std::string_view object_type_name = "TestHandle";

  TestHandle() = default;
};

TEST_CASE("CAPIHandle: is_handle_valid") {
  TestHandle* x = nullptr;
  CHECK_FALSE(is_handle_valid(x));
  x = make_handle<TestHandle>();
  CHECK(is_handle_valid(x));
  break_handle(x);
  CHECK_FALSE(is_handle_valid(x));
}

TEST_CASE("CAPIHandle: ensure_handle_is_valid") {
  TestHandle* x = nullptr;
  CHECK_THROWS_WITH(
      ensure_handle_is_valid(x), "Invalid TileDB TestHandle object");
  x = make_handle<TestHandle>();
  CHECK_NOTHROW(ensure_handle_is_valid(x));
  break_handle(x);
  CHECK_THROWS_WITH(
      ensure_handle_is_valid(x), "Invalid TileDB TestHandle object");
}
