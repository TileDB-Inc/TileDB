/**
 * @file source_utils.h
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
 * Definitions for detecting source locations. Uses C++ std::source_location
 * when available, otherwise uses the older __PRETTY_FUNCTION__ style macros.
 */

#ifndef TILEDB_COMMON_SOURCE_UTILS
#define TILEDB_COMMON_SOURCE_UTILS

#ifdef __cpp_lib_source_location
#include <source_location>
using TileDBSourceLocation = std::source_location;
#define TILEDB_SOURCE_LOCATION() TileDBSourceLocation::current()
#else
class TileDBSourceLocation {
 public:
  TileDBSourceLocation(
      const std::string& file_name,
      uint32_t line,
      uint32_t column,
      const std::string& function_name)
      : file_name_(file_name)
      , line_(line)
      , column_(column)
      , function_name_(function_name) {
  }

  inline const std::string& file_name() const {
    return file_name_;
  }

  inline uint32_t line() const {
    return line_;
  }

  inline uint32_t column() const {
    return column_;
  }

  inline const std::string& function_name() const {
    return function_name_;
  }

 private:
  std::string file_name_;
  uint32_t line_;
  uint32_t column_;
  std::string function_name_;
};

#if defined(__clang__) || defined(__GNUC__)
#define TILEDB_FUNC_SIG __PRETTY_FUNCTION__
#elif defined(_MSC_VER)
#define TILEDB_FUNC_SIG __FUNCSIG__
#endif

#define TILEDB_SOURCE_LOCATION() \
  TileDBSourceLocation(__FILE__, __LINE__, 0, TILEDB_FUNC_SIG)

#endif  // __cpp_lib_source_location

#endif  // TILEDB_COMMON_SOURCE_UTILS
