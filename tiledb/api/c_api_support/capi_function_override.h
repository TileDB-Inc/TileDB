/**
 * @file tiledb/api/c_api_support/capi_function_override.h
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

#ifndef TILEDB_CAPI_LOGGING_ASPECT_H
#define TILEDB_CAPI_LOGGING_ASPECT_H

#include <string>

#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"

/**
 * Default trait class declaration has no members; it's only defined a
 * specialization.
 */
template <auto f>
struct CAPIFunctionNameTrait {
  static constexpr bool exists = false;
};

/*
 * CAPI_PREFIX is an extension point within the macros for defining C API
 * interface functions. The override macro defines a trait class for each C API
 * implementation function. The trait class is a specialization of an otherwise
 * undefined class template. Each trait class contains the name of its
 * implementation function; the name is subsequently picked up by `class
 * LoggingAspect`.
 */
#undef CAPI_PREFIX
#define CAPI_PREFIX(root)                                    \
  template <>                                                \
  struct CAPIFunctionNameTrait<tiledb::api::tiledb_##root> { \
    static constexpr bool exists = true;                     \
    static constexpr const char* name = "tiledb_" #root;     \
  };

struct TileDBToString {
  template<typename T, std::enable_if_t<std::is_arithmetic_v<T>, bool> = true>
  TileDBToString(T val) {
    val_ = std::to_string(val);
  }

  template<typename T, std::enable_if_t<std::is_enum_v<T>, bool> = true>
  TileDBToString(T val) {
    val_ = std::to_string(val);
  }

  template<typename T, std::enable_if_t<std::is_pointer_v<T>, bool> = true>
  TileDBToString(T val) {
    val_ = ptr_to_string(val);
  }

  inline std::string ptr_to_string(const void* ptr) {
    std::stringstream ss;
    ss << ptr;
    return ss.str();
  }

  inline std::string ptr_to_string(void (*ptr)(void*)) {
    std::stringstream ss;
    ss << ptr;
    return ss.str();
  }

  inline std::string ptr_to_string(int (*ptr)(const char *, void *)) {
    std::stringstream ss;
    ss << ptr;
    return ss.str();
  }

  inline std::string ptr_to_string(int (*ptr)(const char *, tiledb_object_t, void *)) {
    std::stringstream ss;
    ss << ptr;
    return ss.str();
  }

  inline const std::string& string() {
    return val_;
  }

  std::string val_;
};

std::string
fmt_args(std::convertible_to<TileDBToString> auto&& ...args)
{
  std::stringstream ss;
  ss << "(";
  size_t i = 0;
  for (auto v : std::initializer_list<TileDBToString>{ args... }) {
    if (i > 0) {
      ss << ", ";
    }
    ss << v.string();
    i++;
  }
  ss << ")";
  return ss.str();
}

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
class LoggingAspect<f> {
 public:
  /**
   * Record the name of the function is the "log entry"
   *
   * @param ... Arguments are ignored
   */
  static void call(Args...args) {
    if constexpr (CAPIFunctionNameTrait<f>::exists) {
      std::string tag{"capi: "};
      auto str_args = fmt_args(args...);
      LOG_ERROR(tag + CAPIFunctionNameTrait<f>::name + str_args);
    }
  }
};

/**
 * Specialization of the aspect selector to `void` overrides the default (the
 * null aspect) to compile with the logging aspect instead.
 */
template <auto f>
struct tiledb::api::CAPIFunctionSelector<f, void> {
  using aspect_type = LoggingAspect<f>;
};

#endif  // TILEDB_EXCEPTION_WRAPPER_TEST_LOGGING_ASPECT_H
