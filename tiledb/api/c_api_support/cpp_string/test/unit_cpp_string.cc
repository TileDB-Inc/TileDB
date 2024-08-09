/*
 * @file tiledb/api/c_api_support/cpp_string/test/unit_cpp_string.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include "../cpp_string.h"

// -------------------------------------------------------
// to_string_view_internal
// -------------------------------------------------------
/*
 * Proxy to move the function under test into the global namespace
 */
template <size_t N = tiledb::api::default_max_c_string_length>
std::string_view to_cpp_string_view_internal(const char* p) {
  /*
   * We launder the pointer to avoid overzealous optimizers from noticing that
   * we're passing short strings to a function with large bounds for what it
   * might access.
   */
  const char* q{std::launder(p)};
  return tiledb::api::to_string_view_internal<N>(q);
}

TEST_CASE("C API Support to_string_view_internal - max default, null input") {
  auto sv{to_cpp_string_view_internal(nullptr)};
  CHECK(sv.data() == nullptr);
  CHECK(sv.length() == 0);
}

TEST_CASE("C API Support to_string_view_internal - max default, empty string") {
  const char empty_string[]{""};
  auto sv{to_cpp_string_view_internal(empty_string)};
  REQUIRE(sv.data() != nullptr);
  CHECK(sv.data() == empty_string);
  CHECK(sv.length() == 0);
}

TEST_CASE(
    "C API Support to_string_view_internal - max default, length 1 string") {
  const char one_string[]{"X"};
  auto sv{to_cpp_string_view_internal(one_string)};
  REQUIRE(sv.data() != nullptr);
  CHECK(sv.data() == one_string);
  CHECK(sv.length() == 1);
}

TEST_CASE("C API Support to_string_view_internal - max 0, null input") {
  auto sv{to_cpp_string_view_internal<0>(nullptr)};
  CHECK(sv.data() == nullptr);
  CHECK(sv.length() == 0);
}

TEST_CASE("C API Support to_string_view_internal - max 0, empty string") {
  const char empty_string[]{""};
  static_assert(sizeof(empty_string) == 1);
  auto sv{to_cpp_string_view_internal<0>(empty_string)};
  REQUIRE(sv.data() != nullptr);
  CHECK(sv.data() == empty_string);
  CHECK(sv.length() == 0);
}

TEST_CASE("C API Support to_string_view_internal - max 0, length 1 string") {
  /*
   * The first character is not NUL, so the string is considered unterminated.
   * The only valid input when template argument is 0 are zero-length strings.
   */
  const char one_string[]{"X"};
  auto sv{to_cpp_string_view_internal<0>(one_string)};
  CHECK(sv.data() == nullptr);
  CHECK(sv.data() == 0);
}

TEST_CASE("C API Support to_string_view_internal - max 2, null input") {
  auto sv{to_cpp_string_view_internal<2>(nullptr)};
  CHECK(sv.data() == nullptr);
  CHECK(sv.length() == 0);
}

TEST_CASE("C API Support to_string_view_internal - max 2, empty string") {
  const char empty_string[]{""};
  static_assert(sizeof(empty_string) == 1);
  auto sv{to_cpp_string_view_internal<2>(empty_string)};
  REQUIRE(sv.data() != nullptr);
  CHECK(sv.data() == empty_string);
  CHECK(sv.length() == 0);
}

TEST_CASE("C API Support to_string_view_internal - max 2, length 1 string") {
  const char one_string[]{"1"};
  auto sv{to_cpp_string_view_internal<2>(one_string)};
  CHECK(sv.data() == one_string);
  CHECK(sv.length() == 1);
}

TEST_CASE("C API Support to_string_view_internal - max 2, length 2 string") {
  // Not terminated within bounds
  const char two_string[]{"12"};
  auto sv{to_cpp_string_view_internal<2>(two_string)};
  CHECK(sv.data() == two_string);
  CHECK(sv.length() == 2);
}

TEST_CASE("C API Support to_string_view_internal - max 2, length 3 string") {
  // Not terminated within bounds
  const char three_string[]{"123"};
  auto sv{to_cpp_string_view_internal<0>(three_string)};
  CHECK(sv.data() == nullptr);
  CHECK(sv.data() == 0);
}

TEST_CASE("C API Support to_string_view_internal - max 2, length 4 string") {
  // Not terminated within bounds
  const char four_string[]{"1234"};
  auto sv{to_cpp_string_view_internal<0>(four_string)};
  CHECK(sv.data() == nullptr);
  CHECK(sv.data() == 0);
}

// -------------------------------------------------------
// to_string_view
// -------------------------------------------------------

template <tiledb::api::StringConstant description>
inline std::string_view to_string_view(const char* p) {
  // See above for rationale for `launder`
  const char* q{std::launder(p)};
  return tiledb::api::to_string_view<description>(q);
}

TEST_CASE("C API Support to_string_view - null input") {
  CHECK_THROWS(to_string_view<"xyzzy">(nullptr));
  CHECK_THROWS_WITH(
      to_string_view<"xyzzy">(nullptr),
      Catch::Matchers::ContainsSubstring("xyzzy"));
}

TEST_CASE("C API Support to_string_view - empty string") {
  const char empty_string[]{""};
  auto sv{to_string_view<"a">(empty_string)};
  CHECK(sv.data() == empty_string);
  CHECK(sv.length() == 0);
}

TEST_CASE("C API Support to_string_view - length 3 string") {
  const char three_string[]{"123"};
  auto sv{to_string_view<"a">(three_string)};
  CHECK(sv.data() == three_string);
  CHECK(sv.length() == 3);
}

TEST_CASE("C API Support to_string_view - invalid candidate") {
  /*
   * A block of memory initialized without NUL characters, large enough to
   * trigger an overflow.
   */
  std::vector<char> bad_data(tiledb::api::default_max_c_string_length + 1, 'X');
  auto p{bad_data.data()};
  CHECK_THROWS(to_string_view<"xyzzy">(p));
  CHECK_THROWS_WITH(
      to_string_view<"xyzzy">(p), Catch::Matchers::ContainsSubstring("xyzzy"));
}
