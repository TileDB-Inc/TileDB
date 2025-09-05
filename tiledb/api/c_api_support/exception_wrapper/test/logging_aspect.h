/**
 * @file tiledb/api/c_api_support/exception_wrapper/test/logging_aspect.h
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
 * This file defines a logging aspect sufficient to verify that the C API
 * aspect system in the exception wrapper is working.
 */

#ifndef TILEDB_EXCEPTION_WRAPPER_TEST_LOGGING_ASPECT_H
#define TILEDB_EXCEPTION_WRAPPER_TEST_LOGGING_ASPECT_H

#include <string>

/**
 * Default trait class declaration has no members; it's only defined a
 * specialization.
 */
template <auto f>
struct CAPIFunctionNameTrait;

/**
 * Base class for the logging aspect class template.
 *
 * This class mimics a global logger. It's rudimentary but suffices for testing.
 * The log is simply a static list of messages.
 */
class LABase {
 protected:
  /**
   * The "log entries".
   */
  static std::vector<std::string> msg_;
  /**
   * Whether `call` has been called since the last reset.
   */
  static bool touched_;

 public:
  /**
   * This isn't a C.41 class, as it's a wrapper around some static variables. In
   * lieu of a real construct, we have a reset function.
   */
  static void reset() {
    msg_.clear();
    touched_ = false;
  }

  /**
   * Append a message to the log
   */
  static void log(std::string_view message) {
    msg_.push_back(std::string(message));
  }

  /** Accessor for the "log entry" */
  static inline const std::vector<std::string>& message() {
    return msg_;
  }
  /** Accessor for the call history flag */
  static inline bool touched() {
    return touched_;
  }
};

/**
 * Logging aspect for the exception wrapper from a C API function
 *
 * @tparam f C API implementation function
 */
template <auto f>
class LoggingAspect;

/**
 * Specialization of the logging aspect that infers the return type and argument
 * types of the template parameter.
 *
 * @tparam R The return type of `f`
 * @tparam Args The argument types of `f`
 * @tparam f C API implementation function
 */
template <class R, class... Args, R (*f)(Args...)>
class LoggingAspect<f> : public LABase {
 public:
  /**
   * Record the name of the function is the "log entry"
   *
   * @param ... Arguments are ignored
   */
  static void call(Args...) {
    log(std::string(CAPIFunctionNameTrait<f>::name));
    touched_ = true;
  }
};

/**
 * Tracing aspect for the exception wrapper from a C API function.
 * Records entry and exit from the function.
 *
 * @tparam f C API implementation function
 */
template <auto f>
class TracingAspect;

template <class R, class... Args, R (*f)(Args...)>
class TracingAspect<f> : public LABase {
 public:
  struct Scope {
    ~Scope() {
      TracingAspect<f>::log(
          std::string(CAPIFunctionNameTrait<f>::name) + "::exit");
    }
  };

  /**
   * Record an entry into the function and return a handle
   * which will record exit from it
   *
   * @param ... Arguments are ignored
   */
  static std::unique_ptr<Scope> call(Args...) {
    touched_ = true;
    log(std::string(CAPIFunctionNameTrait<f>::name) + "::entry");
    return std::make_unique<Scope>();
  }
};

#endif  // TILEDB_EXCEPTION_WRAPPER_TEST_LOGGING_ASPECT_H
