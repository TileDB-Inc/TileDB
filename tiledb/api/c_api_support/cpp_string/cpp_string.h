/*
 * @file tiledb/api/c_api_support/cpp_string/cpp_string.h
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

#ifndef TILEDB_CAPI_SUPPORT_CPP_STRING_H
#define TILEDB_CAPI_SUPPORT_CPP_STRING_H

#include <algorithm>                 // std::copy_n
#include <cstring>                   // std::memchr
#include <string>                    // std::string_view
#include "../argument_validation.h"  // CAPIException

namespace tiledb::api {

/**
 * The default maximum length of C strings accepted by the C API
 *
 * This value is used as a template argument, should there ever be a need to
 * use something other than the default length. A large class of uses are for
 * the names of query fields; at present these do not have a defined maximum
 * admissible length, but ought to have. This default value is large enough for
 * most purposes.
 */
constexpr size_t default_max_c_string_length = 65534;

/**
 * Validate that a pointer is to a C-style string and convert it to a string
 * view.
 *
 * This function is a defense against malformed inputs coming in through the C
 * API. This function validates the following properties of the candidate input:
 *   - The pointer is not null. (It points to something.)
 *   - The string is null-terminated. (It has a length.)
 *   - The string length is admissible. (It's short enough.)
 * The admissible string length might vary depending on how it's being used.
 *
 * This function does not throw. It returns an empty string if there's a
 * validation error.
 *
 * @section Maturity
 *
 * There's one failure that this function cannot withstand at present. If the
 * pointer is to a non-terminated string at the end of a page, there will be an
 * access violation fault that necessarily happens during the search for a
 * terminating NUL character. It would be possible to recover from such an error
 * by catching the resulting signal and causing this function to simply return
 * false. This behavior, however, is not nearly as simple as it might appear.
 *
 * The proper requirement for such behavior is that it be able to deal with
 * multiple simultaneous invocations of the function. As this function is
 * defensive, it should be able to handle many simultaneous calls correctly, as
 * an intentional attacker might generate them specifically with an eye to
 * cause the defense to fault.
 *
 * @param candidate_c_string A pointer to be validated as a null-terminated
 *   character string
 * @return A string view corresponding to the candidate if it is valid. A null
 *   string view (`.data() == nullptr`) otherwise.
 */
template <size_t N = default_max_c_string_length>
std::string_view to_string_view_internal(
    const char* candidate_c_string) noexcept {
  if (candidate_c_string == nullptr) {
    return {};
  }
  /*
   * Note that we search for N+1 characters. In particular, zero-length C-style
   * strings require 1 character of storage for the terminating NUL.
   */
  auto p{
      static_cast<const char*>(std::memchr(candidate_c_string, '\0', N + 1))};
  if (p == nullptr) {
    return {};
  }
  return {candidate_c_string, static_cast<size_t>(p - candidate_c_string)};
}

/**
 * A structural type, holding a string, that can be used as a template argument.
 *
 * @tparam N length of the string
 */
template <size_t N>
struct StringConstant {
  /**
   * The string itself and a terminating null character
   */
  char data_[N];

  /**
   * Ordinary constructor whose argument is expected to be a string literal.
   *
   * @param string A null terminated string.
   */
  // NOLINTNEXTLINE(google-explicit-constructor)
  constexpr StringConstant(const char (&string)[N]) {
    std::copy_n(string, N, data_);
  }

  // NOLINTNEXTLINE(google-explicit-constructor)
  consteval operator std::string_view() const {
    return {data_, N - 1};
  }
};

/**
 * String conversion function that throws errors on validation.
 *
 * @tparam description The description of the string for error messages
 * @param candidate_c_string Possibly a pointer to a C-style string
 * @return
 */
template <StringConstant description>
inline std::string_view to_string_view(const char* candidate_c_string) {
  if (candidate_c_string == nullptr) {
    std::string_view d{description};
    throw CAPIException("Pointer to " + std::string(d) + " may not be NULL");
  }
  auto sv{to_string_view_internal(candidate_c_string)};
  if (sv.data() == nullptr) {
    std::string_view d{description};
    throw CAPIException(
        "Invalid " + std::string(d) + "; no terminating NUL character");
  }
  return sv;
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_SUPPORT_CPP_STRING_H
