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
 * This file defines macros to define C API functions.
 */

#ifndef TILEDB_EXCEPTION_WRAPPER_TEST_LOGGING_ASPECT_H
#define TILEDB_EXCEPTION_WRAPPER_TEST_LOGGING_ASPECT_H

#include <string>

template <auto f>
struct CAPIFunctionMetadata;

template <class R, class... Args, R (*f)(Args...)>
struct CAPIFunctionMetadata<f>;

/**
 * Base class for the logging aspect class template.
 *
 * This class mimics a global logger. It's rudimentary but suffices for testing.
 * Instead of a growing log, it has a single "log entry"
 */
class LABase {
 protected:
  static std::string msg_;
  static bool touched_;

 public:
  static void reset() {
    msg_ = "";
    touched_ = false;
  }
  static inline std::string message() {
    return msg_;
  }
  static inline bool touched() {
    return touched_;
  }
};

template <auto f>
class LoggingAspect;

template <class R, class... Args, R (*f)(Args...)>
class LoggingAspect<f> : public LABase {
 public:
  static void call(Args...) {
    msg_ = CAPIFunctionMetadata<f>::name;
    touched_ = true;
  }
};

#endif  // TILEDB_EXCEPTION_WRAPPER_TEST_LOGGING_ASPECT_H
