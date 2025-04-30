/**
 * @file   arithmetic.h
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
 * This file defines common non-primitive arithmetic functions.
 */
#ifndef TILEDB_ARITHMETIC_H
#define TILEDB_ARITHMETIC_H

#include "tiledb/common/unreachable.h"

#include <limits>
#include <optional>

namespace tiledb::common {

/**
 * Provides the member typedef `type` which is the 64-bit integral type
 * with the same signed-ness as the integral type `T`,
 * if `T` itself is narrower than 64 bits.
 */
template <typename T>
struct extended_integral_type;

template <>
struct extended_integral_type<int8_t> {
  using type = int64_t;
};

template <>
struct extended_integral_type<int16_t> {
  using type = int64_t;
};

template <>
struct extended_integral_type<int32_t> {
  using type = int64_t;
};

template <>
struct extended_integral_type<uint8_t> {
  using type = uint64_t;
};

template <>
struct extended_integral_type<uint16_t> {
  using type = uint64_t;
};

template <>
struct extended_integral_type<uint32_t> {
  using type = uint64_t;
};

/**
 * Provides the member typedef `type` which is the 64-bit integral type
 * with the same signed-ness as the integral type `T`.
 */
template <typename T>
struct integral_64 {
  using type = extended_integral_type<T>::type;
};

template <>
struct integral_64<uint64_t> {
  using type = uint64_t;
};

template <>
struct integral_64<int64_t> {
  using type = int64_t;
};

/**
 * Provides functions for performing common arithmetic operations
 * checking overflow.
 */
template <typename T>
struct checked_arithmetic {
  /**
   * @return `Some(a + b)` if it can be represented as type `T`, `None`
   * otherwise.
   */
  static std::optional<T> add(T a, T b) {
    const typename extended_integral_type<T>::type a64 = a;
    const typename extended_integral_type<T>::type b64 = b;
    const typename extended_integral_type<T>::type c64 = a64 + b64;

    if (std::numeric_limits<T>::lowest() <= c64 &&
        c64 <= std::numeric_limits<T>::max()) {
      return static_cast<T>(c64);
    } else {
      return std::nullopt;
    }
  }

  /**
   * @return `Some(a - b)` if it can be represented as type `T`, `None`
   * otherwise.
   */
  static std::optional<T> sub(T a, T b) {
    const typename extended_integral_type<T>::type a64 = a;
    const typename extended_integral_type<T>::type b64 = b;
    const typename extended_integral_type<T>::type c64 = a64 - b64;

    if (std::numeric_limits<T>::lowest() <= c64 &&
        c64 <= std::numeric_limits<T>::max()) {
      return static_cast<T>(c64);
    } else {
      return std::nullopt;
    }
  }
};

template <>
struct checked_arithmetic<uint64_t> {
  static std::optional<uint64_t> add(uint64_t a, uint64_t b) {
    if (a <= std::numeric_limits<uint64_t>::max() - b) {
      return a + b;
    } else {
      return std::nullopt;
    }
  }

  static std::optional<uint64_t> sub(uint64_t a, uint64_t b) {
    if (a < b) {
      return std::nullopt;
    } else {
      return a - b;
    }
  }

  /**
   * @return `a - b` if it can be represented as an `int64_t` without undefined
   * behavior, `std::nullopt` otherwise
   */
  static std::optional<int64_t> sub_signed(uint64_t a, uint64_t b) {
    if (a >= b) {
      // since `b >= 0` due to unsigned-ness this will not underflow
      // but we do have to check if it can be represented as `int64_t`
      if (a - b > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return std::nullopt;
      } else {
        return a - b;
      }
    }

    // NB: a traditional and "obvious" way of detecting overflow
    // is using the signed-ness of the subtraction result compared
    // against the inputs.  This is actually undefined behavior
    // and could also possibly be optimized out by a compiler
    // which would make us sad.
    //
    // We will instead rely on `a - b == -(b - a)` which decays to the
    // above case, and we can check for the only possible
    // result which we don't have to negate.
    const auto negated = sub_signed(b, a);
    if (!negated.has_value()) {
      // edge case: the delta is exactly the non-negatable twos complement value
      if (b - a ==
          static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1) {
        return std::numeric_limits<int64_t>::min();
      } else {
        return std::nullopt;
      }
    } else {
      return -negated.value();
    }
  }
};

template <>
struct checked_arithmetic<int64_t> {
  static std::optional<int64_t> add(int64_t a, int64_t b) {
    if (b >= 0) {
      if (a <= std::numeric_limits<int64_t>::max() - b) {
        return a + b;
      } else {
        return std::nullopt;
      }
    } else {
      if (std::numeric_limits<int64_t>::min() - b <= a) {
        return a + b;
      } else {
        return std::nullopt;
      }
    }
  }

  static std::optional<int64_t> sub(int64_t a, int64_t b) {
    if (a < b) {
      // negate so we can apply logic with generality
      const auto negated = sub(b, a);
      if (!negated.has_value()) {
        if (a - b == std::numeric_limits<int64_t>::min()) {
          return std::numeric_limits<int64_t>::min();
        } else {
          return std::nullopt;
        }
      } else if (negated.value() == std::numeric_limits<int64_t>::min()) {
        return std::nullopt;
      } else {
        return -negated.value();
      }
    } else {
      // overflow condition `a - b > i64::MAX`
      // is equivalent to `a > i64::MAX + b`
      if (b >= 0) {
        return a - b;
      } else if (a > std::numeric_limits<int64_t>::max() + b) {
        return std::nullopt;
      } else {
        return a - b;
      }
    }
  }

  static std::optional<int64_t> sub_signed(int64_t a, int64_t b) {
    return sub(a, b);
  }
};

}  // namespace tiledb::common

#endif
