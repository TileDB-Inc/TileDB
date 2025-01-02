/**
 * @file tdb_rapidcheck.h
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
 * This file wraps <rapidcheck.h> and <rapidcheck/catch.h> for convenience
 * (and compatibility due to a snag with <rapidcheck/catch.h>).
 */

#ifndef TILEDB_MISC_TDB_RAPIDCHECK_H
#define TILEDB_MISC_TDB_RAPIDCHECK_H

/*
 * `catch` must be included first to define
 * `CATCH_TEST_MACROS_HPP_INCLUDED`
 * which is the bridge between v2 and v3 compatibility for <rapidcheck/catch.h>
 */
#include <test/support/tdb_catch.h>

#include <rapidcheck.h>
#include <rapidcheck/catch.h>

#include <type_traits>

namespace tiledb::test {

/**
 * Marker that a template is instantiated by top-level CATCH code
 */
struct AsserterCatch {};

/**
 * Marker that a template is instantiated by a rapidcheck property
 */
struct AsserterRapidcheck {};

}  // namespace tiledb::test

/**
 * Helper macro for running an assert in a context
 * where it could be either in a top-level catch test,
 * or in a rapidcheck property
 * (e.g. a function which could be called from either)
 *
 * Expects a type named `Asserter` to be either `AsserterCatch` or
 * `AsserterRapidcheck`.
 *
 * This expands to REQUIRE for `AsserterCatch` and RC_ASSERT for
 * `AsserterRapidcheck`. For both type markers this will throw an exception.
 */
#define RCCATCH_REQUIRE(...)                                              \
  do {                                                                    \
    static_assert(                                                        \
        std::is_same<Asserter, tiledb::test::AsserterCatch>::value ||     \
        std::is_same<Asserter, tiledb::test::AsserterRapidcheck>::value); \
    if (std::is_same<Asserter, tiledb::test::AsserterCatch>::value) {     \
      REQUIRE(__VA_ARGS__);                                               \
    } else {                                                              \
      RC_ASSERT(__VA_ARGS__);                                             \
    }                                                                     \
  } while (0)

/**
 * Helper macro for running an assert in a context
 * where it could be either in a top-level catch test,
 * or in a rapidcheck property
 * (e.g. a function which could be called from either)
 *
 * Expects a type named `Asserter` to be either `AsserterCatch` or
 * `AsserterRapidcheck`.
 *
 * This expands to CHECK for `AsserterCatch` and RC_ASSERT for
 * `AsserterRapidcheck`. For `AsserterCatch`, this will not throw an exception -
 * the test will continue. For `AsserterRapidcheck` this will throw an
 * exception.
 */
#define RCCATCH_CHECK(...)                                                \
  do {                                                                    \
    static_assert(                                                        \
        std::is_same<Asserter, tiledb::test::AsserterCatch>::value ||     \
        std::is_same<Asserter, tiledb::test::AsserterRapidcheck>::value); \
    if (std::is_same<Asserter, tiledb::test::AsserterCatch>::value) {     \
      CHECK(__VA_ARGS__);                                                 \
    } else {                                                              \
      RC_ASSERT(__VA_ARGS__);                                             \
    }                                                                     \
  } while (0)

#define RCCATCH_THROWS(...)                                               \
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

namespace tiledb::test::tdbrc {

/**
 * Wrapper struct whose `Arbitrary` specialization returns
 * a non-shrinking generator.
 *
 * This is meant to be used for generators which have a
 * very very large shrinking space, such that by default
 * we do not want to shrink (e.g. in CI - instead we want
 * to capture the seed immediately and file a bug report
 * where the assignee can kick off the shrinking).
 */
template <typename T>
struct NonShrinking {
  NonShrinking(T&& inner)
      : inner_(inner) {
  }

  T inner_;

  operator T&() {
    return inner_;
  }

  operator const T&() const {
    return inner_;
  }
};

}  // namespace tiledb::test::tdbrc

namespace rc {
template <typename T>
struct Arbitrary<tiledb::test::tdbrc::NonShrinking<T>> {
  static Gen<tiledb::test::tdbrc::NonShrinking<T>> arbitrary() {
    auto inner = gen::noShrink(gen::arbitrary<T>());
    return gen::apply(
        [](T inner) {
          return tiledb::test::tdbrc::NonShrinking<T>(std::move(inner));
        },
        inner);
  }
};
}  // namespace rc

#endif  // TILEDB_MISC_TDB_RAPIDCHECK_H
