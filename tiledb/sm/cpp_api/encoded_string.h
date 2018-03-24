/**
 * @file   encoded_string.h
 *
 * @author Ravi Gaddipati
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
 * This defines a tiledb type that encapsulates a string with an encoding.
 */

#ifndef TILEDB_STRING_H
#define TILEDB_STRING_H

#include "tiledb.h"

#include <string>

namespace tiledb {

enum class Encoding { ASCII, UTF8, UTF16, UTF32, UCS2, UCS4 };

/**
 * Encapsulates a string with an encoding. No transcoding or
 * conversion is done, a std::basic_string<T> is tagged for
 * later use. T should be chosen to support the encoding.
 * @tparam T Underlying char type
 * @tparam encoding String encoding
 */
template <typename T, Encoding E>
struct EncodedString {
  using str_type = std::basic_string<T>;
  static constexpr Encoding encoding = E;

  explicit EncodedString(std::string str)
      : str(std::move(str)) {
  }
  explicit EncodedString(const T* c)
      : str(c) {
  }

  str_type str;
};

namespace impl {

template <Encoding E>
struct encoding_to_tiledb;

template <>
struct encoding_to_tiledb<Encoding::ASCII> {
  static constexpr tiledb_datatype_t tiledb_type = TILEDB_STRING_ASCII;
};

template <>
struct encoding_to_tiledb<Encoding::UTF8> {
  static constexpr tiledb_datatype_t tiledb_type = TILEDB_STRING_UTF8;
};

template <>
struct encoding_to_tiledb<Encoding::UTF16> {
  static constexpr tiledb_datatype_t tiledb_type = TILEDB_STRING_UTF16;
};

template <>
struct encoding_to_tiledb<Encoding::UTF32> {
  static constexpr tiledb_datatype_t tiledb_type = TILEDB_STRING_UTF32;
};

template <>
struct encoding_to_tiledb<Encoding::UCS2> {
  static constexpr tiledb_datatype_t tiledb_type = TILEDB_STRING_UCS2;
};

template <>
struct encoding_to_tiledb<Encoding::UCS4> {
  static constexpr tiledb_datatype_t tiledb_type = TILEDB_STRING_UCS4;
};

}  // namespace impl

}  // namespace tiledb

#endif  // TILEDB_STRING_H
