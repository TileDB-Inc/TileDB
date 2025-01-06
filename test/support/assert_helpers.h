/**
 * @file assert_helpers.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 * Provides definitions of macros which enable dispatching assertion
 * failures to library code, depending on the `Asserter`
 * type in the scope of the caller.
 *
 * This is desirable for writing code which might want to be
 * invoked in the context of multiple different libraries, such
 * as a helper function that could be called from a Catch2 TEST_CASE
 * or a rapidcheck property.
 */

#ifndef TILEDB_ASSERT_HELPERS_H
#define TILEDB_ASSERT_HELPERS_H

#include <test/support/tdb_catch.h>
#include <test/support/tdb_rapidcheck.h>

#include <stdexcept>

namespace tiledb::test {

/**
 * Marker that a template is instantiated by top-level CATCH code
 */
struct AsserterCatch {};

/**
 * Marker that a template is instantiated by a rapidcheck property
 */
struct AsserterRapidcheck {};

/**
 * Marker that a template is instantiated by application code
 */
struct AsserterRuntimeException {};

}  // namespace tiledb::test

#define __STR_(x) #x
#define __STR_VA_(...) __STR_(__VA_ARGS__)

/**
 * Helper macro for running an assert in a context
 * where we might want to dispatch assertion failure behavior
 * to one of a few possible different libraries depending
 * on the caller
 * (e.g. a function which can be called from either a Catch2
 * TEST_CASE or from a rapidcheck property wants to use REQUIRE
 * and RC_ASSERT respectively).
 *
 * Expects a type named `Asserter` to be either `AsserterCatch` or
 * `AsserterRapidcheck`.
 *
 * This expands to REQUIRE for `AsserterCatch` and RC_ASSERT for
 * `AsserterRapidcheck`. For both type markers this will throw an exception.
 */
#define ASSERTER(...)                                                      \
  do {                                                                     \
    static_assert(                                                         \
        std::is_same<Asserter, tiledb::test::AsserterCatch>::value ||      \
        std::is_same<Asserter, tiledb::test::AsserterRapidcheck>::value || \
        std::is_same<Asserter, tiledb::test::AsserterRuntimeException>::   \
            value);                                                        \
    if (std::is_same<Asserter, tiledb::test::AsserterCatch>::value) {      \
      REQUIRE(__VA_ARGS__);                                                \
    } else if (std::is_same<Asserter, tiledb::test::AsserterRapidcheck>::  \
                   value) {                                                \
      RC_ASSERT(__VA_ARGS__);                                              \
    } else {                                                               \
      if (!(__VA_ARGS__)) {                                                \
        throw std::runtime_error(                                          \
            std::string("Assertion failed: ") +                            \
            std::string(__STR_VA_(__VA_ARGS__)));                          \
      }                                                                    \
    }                                                                      \
  } while (0)

/**
 * Helper macro for asserting that an expression throws an exception
 * in a context where we might want to dispatch assertion failure behavior
 * to one of a few possible different libraries depending on the caller
 * (e.g. a function which can be called from either a Catch2
 * TEST_CASE or from a rapidcheck property wants to use REQUIRE
 * and RC_ASSERT respectively).
 *
 * Expects a type named `Asserter` to be either `AsserterCatch` or
 * `AsserterRapidcheck`.
 *
 * This expands to REQUIRE_THROWS for `AsserterCatch` and RC_ASSERT_THROWS for
 * `AsserterRapidcheck`. For both type markers this will throw an exception.
 */
#define ASSERTER_THROWS(...)                                              \
  do {                                                                    \
    static_assert(                                                        \
        std::is_same<Asserter, tiledb::test::AsserterCatch>::value ||     \
        std::is_same<Asserter, tiledb::test::AsserterRapidcheck>::value); \
    if (std::is_same<Asserter, tiledb::test::AsserterCatch>::value) {     \
      REQUIRE_THROWS(__VA_ARGS__);                                        \
    } else {                                                              \
      RC_ASSERT_THROWS(__VA_ARGS__);                                      \
    }                                                                     \
  } while (0)

#endif
