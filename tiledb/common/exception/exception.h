/**
 * @file exception.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 */

#ifndef TILEDB_COMMON_EXCEPTION_H
#define TILEDB_COMMON_EXCEPTION_H

#include <stdexcept>

#include <cpptrace/cpptrace.hpp>

#include "tiledb/common/common-std.h"
#include "tiledb/common/status.h"

namespace tiledb::common {

/**
 * An exception class interconvertible with error Status values.
 *
 * By design, this exception class only interconverts with error status, not
 * with the OK status. This class is an exception, after all, and OK cannot be
 * considered an exception state in any reasonable way.
 */
class StatusException : public std::exception {
  // Uses private nothrow conversion constructor
  friend void throw_if_not_ok(const Status&);

  /**
   * Vicinity where exception originated
   */
  std::string origin_;

  /**
   * Specific error message
   */
  std::string message_;

  /**
   * Text returned by `what()`
   *
   * `std::exception:what()` requires the following:
   * (quoted from https://en.cppreference.com/w/cpp/error/exception/what)
   * ```
   * The pointer is guaranteed to be valid at least until the exception object
   * from which it is obtained is destroyed, or until a non-const member
   * function on the exception object is called.
   * ```
   * The contents of `what()` are constructed from `origin_` and `message_`
   * and thus must be stored separately from these. This redundancy is necessary
   * because `Status` has both strings and is interconvertible with this class.
   *
   * This variable can be considered a cache. It's created on demand but doesn't
   * actually change the value of anything, and thus it's declared `mutable`. If
   * it were initialized at construction it would declared `const`.
   */
  mutable std::string what_{};  // always initialized as empty

  /**
   * Conversion constructor from Status.
   *
   * This is the `noexcept` version of this constructor, which would be unsafe
   * if exposed publicly. It's used for conversion where OK status has already
   * been checked.
   *
   * @pre !st.ok()
   * @param st Status from which to convert
   */
  explicit StatusException(const Status& st, std::nothrow_t) noexcept
      : StatusException(std::string(st.origin()), st.message()) {
  }

  /**
   * Internal factory to convert from Status. This factory is required because
   * if a status is in an OK state the constructor will have null pointer
   * errors.
   *
   * @return
   */
  static StatusException make_from_status(const Status& st) {
    if (st.ok()) {
      throw std::invalid_argument("May not construct exception from OK status");
    }
    return StatusException{st, std::nothrow};
  }

 public:
  /**
   * Default constructed is deleted.
   *
   * An empty StatusException is nonsensical. If it were to interconvert with
   * any Status value, it would be an empty Status, which is the OK status.
   * Since this class only represents non-OK status, a default-constructed
   * object would violate that basic class invariant.
   */
  StatusException() = delete;

  /**
   * Ordinary constructor separates origin and error message in order to
   * support subclass constructors.
   *
   * @param code Legacy status code
   * @param what Error message
   * @pre Argument `what` must be a C-style null-terminated string
   */
  StatusException(const std::string origin, const std::string message)
      : origin_(origin)
      , message_(message) {
    //cpptrace::generate_trace().print();
  }

  /**
   * Conversion constructor from Status throws if the status is not an error.
   *
   * @param st Status from which to convert
   */
  explicit StatusException(const Status& st)
      // Invoke the move constructor after factory validation of the argument
      : StatusException(make_from_status(st)) {
    //cpptrace::generate_trace().print();
  }

  /// Default copy constructor
  StatusException(const StatusException&) = default;

  /// Default move constructor
  StatusException(StatusException&&) = default;

  /// Default copy assignment
  StatusException& operator=(const StatusException&) = default;

  /// Default move assignment
  StatusException& operator=(StatusException&&) = default;

  /**
   * Explanatory text about the exception.
   *
   * @return pointer to internal string containing the text
   */
  virtual const char* what() const noexcept override;

  /**
   * Extract a `Status` object from this exception.
   *
   * The lifespan of the status must be shorter than that of this exception from
   * which it is extracted.
   */
  Status extract_status() const {
    return {origin_, message_};
  }
};

/**
 * Program flow conversion from a status to an exception.
 *
 * Returns if the status argument is OK. Throws a converted StatusException
 * otherwise.
 *
 * @post st.ok(). If the status is not OK, this function will have thrown.
 *
 * @param st Status to check and either ignore or throw
 */
inline void throw_if_not_ok(const Status& st) {
  if (!st.ok()) {
    // friend declaration allows calling this private constructor
    throw StatusException(st, std::nothrow);
  }
}

}  // namespace tiledb::common

#endif  // TILEDB_COMMON_EXCEPTION_H
