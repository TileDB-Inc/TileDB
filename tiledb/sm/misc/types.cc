/**
 * @file   types.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file defines common types for Query/Write/Read class usage
 */

#include "tiledb/sm/misc/types.h"

namespace tiledb {
namespace sm {

template <>
std::string ByteVecValue::rvalue_as<std::string>() const {
  return std::string(
      reinterpret_cast<const std::string::value_type*>(x_.data()), x_.size());
}

// Explicit template instantiations
template <class T>
T ByteVecValue::rvalue_as() const {
  return *static_cast<const T*>(static_cast<const void*>(x_.data()));
}

// Explicit template instantiations
template char ByteVecValue::rvalue_as<char>() const;
template int8_t ByteVecValue::rvalue_as<int8_t>() const;
template uint8_t ByteVecValue::rvalue_as<uint8_t>() const;
template int16_t ByteVecValue::rvalue_as<int16_t>() const;
template uint16_t ByteVecValue::rvalue_as<uint16_t>() const;
template int ByteVecValue::rvalue_as<int>() const;
template uint32_t ByteVecValue::rvalue_as<uint32_t>() const;
template int64_t ByteVecValue::rvalue_as<int64_t>() const;
template uint64_t ByteVecValue::rvalue_as<uint64_t>() const;

}  // namespace sm
}  // namespace tiledb
