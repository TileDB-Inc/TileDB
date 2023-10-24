/**
 * @file   helper_type.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This defines TileDB datatypes.
 */

#ifndef TILEDB_TEST_TYPE_H
#define TILEDB_TEST_TYPE_H

namespace tiledb::test {

using Datatype = tiledb::sm::Datatype;

/**
 * Convert a type into a tiledb::sm::Datatype. The default for all copyable
 * types is char.
 */
template <typename T>
struct type_to_tiledb {};

template <>
struct type_to_tiledb<std::byte> {
  using type = std::byte;
  static constexpr Datatype tiledb_type = Datatype::BLOB;
};

template <>
struct type_to_tiledb<bool> {
  using type = bool;
  static constexpr Datatype tiledb_type = Datatype::BOOL;
};

template <>
struct type_to_tiledb<int8_t> {
  using type = int8_t;
  static constexpr Datatype tiledb_type = Datatype::INT8;
};

template <>
struct type_to_tiledb<uint8_t> {
  using type = uint8_t;
  static constexpr Datatype tiledb_type = Datatype::UINT8;
};

template <>
struct type_to_tiledb<int16_t> {
  using type = int16_t;
  static constexpr Datatype tiledb_type = Datatype::INT16;
};

template <>
struct type_to_tiledb<uint16_t> {
  using type = uint16_t;
  static constexpr Datatype tiledb_type = Datatype::UINT16;
};

template <>
struct type_to_tiledb<int32_t> {
  using type = int32_t;
  static constexpr Datatype tiledb_type = Datatype::INT32;
};

template <>
struct type_to_tiledb<uint32_t> {
  using type = uint32_t;
  static constexpr Datatype tiledb_type = Datatype::UINT32;
};

template <>
struct type_to_tiledb<int64_t> {
  using type = int64_t;
  static constexpr Datatype tiledb_type = Datatype::INT64;
};

template <>
struct type_to_tiledb<uint64_t> {
  using type = uint64_t;
  static constexpr Datatype tiledb_type = Datatype::UINT64;
};

template <>
struct type_to_tiledb<float> {
  using type = float;
  static constexpr Datatype tiledb_type = Datatype::FLOAT32;
};

template <>
struct type_to_tiledb<double> {
  using type = double;
  static constexpr Datatype tiledb_type = Datatype::FLOAT64;
};

template <>
struct type_to_tiledb<std::string> {
  using type = char;
  static const Datatype tiledb_type = Datatype::STRING_ASCII;
};

template <typename T>
constexpr Datatype tdb_type = type_to_tiledb<T>::tiledb_type;

}  // namespace tiledb::test

#endif  // TILEDB_TEST_TYPE_H
