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

#include <string_view>

#include "tiledb/sm/misc/types.h"

namespace tiledb {
namespace sm {

template <>
std::string ByteVecValue::rvalue_as<std::string>() const {
  return std::string(
      reinterpret_cast<const std::string::value_type*>(x_.data()), x_.size());
}

template <class T>
common::Interval<T> Range::Interval_from_Range() const {
  return Interval_from_Range<T>(*this);
//  return Interval<T>(
//      Interval<T>::closed,
//      *(const T*)r.start(),
//      *(const T*)r.end(),
//      Interval<T>::closed);
}

template <class T>
common::Interval<T> Range::Interval_from_Range(const Range& r) {
  return Interval<T>(
      Interval<T>::closed,
      *(const T*)r.start(),
      *(const T*)r.end(),
      Interval<T>::closed);
}

template <>
common::Interval<std::string_view> Range::Interval_from_Range<std::string_view>(
    const Range& r) {
  typedef common::Interval<std::string_view> Isv;
  // Interval<std::string_view> Interval_from_Range<std::string_view>(const
  // Range& r) {
  int select = 0;
  if (!r.start_size())  // empty end?
    select += 1;        // yes
  if (!r.end_size())    // empty start?
    select += 2;        // yes

  switch (select) {
    case 0: {  // have both start and end values
      Isv interval(
          Isv::closed,
          std::string_view{(char*)r.start(), r.start_size()},
          std::string_view{(char*)r.end(), r.end_size()},
          Isv::closed);
      return interval;
    }
    case 1: {  // have end but no start value
      Isv interval(
          Isv::minus_infinity,
          std::string_view{(char*)r.end(), r.end_size()},
          Isv::Interval<std::string_view>::closed);
      return interval;
    }
    case 2: {  // have start but no end value
      Isv interval(
          Isv::closed,
          std::string_view{(char*)r.start(), r.start_size()},
          Isv::plus_infinity);
      return interval;
    }
    case 3: {  // have neither start nor end value
      Isv interval(
          Isv::minus_infinity,
          Isv::plus_infinity);
      return interval;
    }
    default: {
      Isv interval(
          Isv::empty_set);
      return interval;
    }

  }  // switch
}

template <>
common::Interval<std::string_view>
Range::Interval_from_Range<std::string_view>() const {
  return Interval_from_Range<std::string_view>(*this);
}

}  // namespace sm
}  // namespace tiledb
