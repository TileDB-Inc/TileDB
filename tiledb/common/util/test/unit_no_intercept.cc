/**
 * @file   unit_no_intercept.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file contains a simple test which validates that intercepts
 * are no-ops when `TILEDB_INTERCEPTS` is not defined.
 */

#undef TILEDB_INTERCEPTS
#include "tiledb/common/util/intercept.h"

#include <test/support/tdb_catch.h>

namespace no_intercept {

DECLARE_INTERCEPT(not_my_library_function_entry, int);
DEFINE_INTERCEPT(not_my_library_function_entry, int);

// declare another symbol with a different type,
// this will not compile if the intercepts are not removed by the preprocessor
int not_my_library_function_entry(void) {
  return 1;
}

DEFINE_INTERCEPT(test_case_body, int);

}  // namespace no_intercept

TEST_CASE("Intercept undef declare and define", "[intercept]") {
  REQUIRE(no_intercept::not_my_library_function_entry() == 1);
}

TEST_CASE("Intercept undef inline", "[intercept]") {
  // An intercept with side effects is a bad idea,
  // but this does illustrate that the preprocessor removes
  // all of the intercept arguments
  int a = 0;
  INTERCEPT(test_case_body, a++);
  REQUIRE(a == 0);

  // and for good measure another symbol with the same name
  volatile int test_case_body = 1;
  REQUIRE(test_case_body == 1);
}
