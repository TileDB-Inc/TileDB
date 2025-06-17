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

#include <algorithm>
#include <cassert>
#include <iostream>
#include <list>

#ifdef _WIN32
std::vector<int> assert_exit_codes{3};
#else
std::vector<int> assert_exit_codes{
    0x6,   /* SIGABRT */
    0x86,  /* also SIGABRT */
    0x8600 /* core dump, which may be caused by SIGABRT */
};
#endif

TEST_CASE("CI: Test assertions configuration", "[ci][assertions]") {
  int retval = system(TILEDB_PATH_TO_TRY_ASSERT);

  // in case value is one not currently accepted, report what was returned.
  std::cout << "retval is " << retval << " (0x" << std::hex << retval
            << ") from " << TILEDB_PATH_TO_TRY_ASSERT << std::endl;

#ifdef TILEDB_ASSERTIONS
  REQUIRE(
      std::find(assert_exit_codes.begin(), assert_exit_codes.end(), retval) !=
      assert_exit_codes.end());
#else
  (void)assert_exit_codes;
  REQUIRE(retval == 0);
#endif
}

TEST_CASE("CI: Test libc assertions configuration", "[ci][assertions]") {
  int retval = system(TILEDB_PATH_TO_TRY_LIBC_ASSERT);

  // report return value
  std::cout << "retval is " << retval << " (0x" << std::hex << retval
            << ") from " << TILEDB_PATH_TO_TRY_LIBC_ASSERT << std::endl;

  // This is a little awkward but comes down to the fact that:
  // 1) on windows the standard library assertions are enabled in the
  //    debug runtime with no clear way to enable them for release
  // 2) on windows/max the standard library assertions are toggled
  //    by a macro, indepdently of the build configuration
#if defined(_WIN32)
#ifndef NDEBUG
  const bool expectAssertFailed = true;
#else
  const bool expectAssertFailed = false;
#endif
#else
#ifdef TILEDB_ASSERTIONS
  const bool expectAssertFailed = true;
#else
  const bool expectAssertFailed = false;
#endif
#endif

  if (expectAssertFailed) {
    REQUIRE(
        std::find(assert_exit_codes.begin(), assert_exit_codes.end(), retval) !=
        assert_exit_codes.end());
  } else {
    REQUIRE(retval == 0);
  }
}
