/**
 * @file test/ci/test_assert
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
 *
 * This file defines a test which calls executable `try_assert` to determine
 * whether assertions are correctly enabled in build tree.
 */

#define CATCH_CONFIG_MAIN
#include <test/support/tdb_catch.h>

#include <cassert>
#include <iostream>

#ifdef _WIN32
constexpr int assert_exit_code = 3;
#else
constexpr int assert_exit_code = 6;
#endif

TEST_CASE("CI: Test assertions configuration", "[ci][assertions]") {
  int retval = system(TILEDB_PATH_TO_TRY_ASSERT "/try_assert");

#ifdef TILEDB_ASSERTIONS
  REQUIRE(retval == assert_exit_code);
#else
  (void)assert_exit_code;
  REQUIRE(retval == 0);
#endif
}