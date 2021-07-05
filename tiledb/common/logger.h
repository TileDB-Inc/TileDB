/**
 * @file   logger.h
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
 * This file defines class Logger, which is implemented as a wrapper around
 * `spdlog`. By policy `spdlog` must remain encapsulated as an implementation
 * and not be exposed as a dependency of the TileDB library. Accordingly, this
 * header should not be included as a header in any other header file. For
 * inclusion in a header (notably for use within the definition of
 * template-dependent functions), include the header `logger_public.h`.
 *
 * The reason for this restriction is a technical limitation in template
 * instantiation. Part of the interface to `spdlog` consists of template
 * functions with variadic template arguments. Instantiation of such function
 * does not instantiate a variadic function (for exmaple `printf`) but rather a
 * function with a fixed number of arguments that depend upon the argument list.
 * Such variadic template argument lists cannot be forwarded across the
 * boundaries of compilation units, so exposing variadic template arguments
 * necessarily exposes the dependency upon `spdlog`. Thus this file `logger.h`,
 * which does have such arguments, must remain entirely within the library, but
 * `logger_public.h`, which does not have such arguments, may be exposed without
 * creating an additional external dependency.
 */

#pragma once
#ifndef TILEDB_LOGGER_H
#define TILEDB_LOGGER_H

#include <spdlog/spdlog.h>

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/status.h"

namespace tiledb {
namespace common {

/** Definition of class Logger. */
class Logger {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Logger();

  /** Destructor. */
  ~Logger();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Log a trace statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void trace(const char* msg);

  /**
   * A formatted trace statment.
   *
   * @param fmt A fmtlib format string, see http://fmtlib.net/latest/ for
   *     details.
   * @param arg positional argument to format.
   * @param args optional additional positional arguments to format.
   */
  template <typename Arg1, typename... Args>
  void trace(const char* fmt, const Arg1& arg1, const Args&... args) {
    logger_->trace(fmt, arg1, args...);
  }

  /**
   * Log a debug statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void debug(const char* msg);

  /**
   * A formatted debug statment.
   *
   * @param fmt A fmtlib format string, see http://fmtlib.net/latest/ for
   *     details.
   * @param arg positional argument to format.
   * @param args optional additional positional arguments to format.
   */
  template <typename Arg1, typename... Args>
  void debug(const char* fmt, const Arg1& arg1, const Args&... args) {
    logger_->debug(fmt, arg1, args...);
  }

  /**
   * Log an info statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void info(const char* msg);

  /**
   * A formatted info statment.
   *
   * @param fmt A fmtlib format string, see http://fmtlib.net/latest/ for
   *     details.
   * @param arg positional argument to format.
   * @param args optional additional positional arguments to format.
   */
  template <typename Arg1, typename... Args>
  void info(const char* fmt, const Arg1& arg1, const Args&... args) {
    logger_->info(fmt, arg1, args...);
  }

  /**
   * Log a warn statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void warn(const char* msg);

  /**
   * A formatted warn statment.
   *
   * @param fmt A fmtlib format string, see http://fmtlib.net/latest/ for
   *     details.
   * @param arg positional argument to format.
   * @param args optional additional positional arguments to format.
   */
  template <typename Arg1, typename... Args>
  void warn(const char* fmt, const Arg1& arg1, const Args&... args) {
    logger_->warn(fmt, arg1, args...);
  }

  /**
   * Log an error with no message formatting.
   *
   * @param msg The string to log
   * */
  void error(const char* msg);

  /** A formatted error statement.
   *
   * @param fmt A fmtlib format string, see http://fmtlib.net/latest/ for
   * details.
   * @param arg1 positional argument to format.
   * @param args optional additional positional arguments to format.
   */
  template <typename Arg1, typename... Args>
  void error(const char* fmt, const Arg1& arg1, const Args&... args) {
    logger_->error(fmt, arg1, args...);
  }

  /**
   * Log a critical statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void critical(const char* msg);

  /**
   * A formatted critical statment.
   *
   * @param fmt A fmtlib format string, see http://fmtlib.net/latest/ for
   *     details.
   * @param arg positional argument to format.
   * @param args optional additional positional arguments to format.
   */
  template <typename Arg1, typename... Args>
  void critical(const char* fmt, const Arg1& arg1, const Args&... args) {
    logger_->critical(fmt, arg1, args...);
  }

  /** Verbosity level. */
  enum class Level : char {
    FATAL,
    ERR,
    WARN,
    INFO,
    DBG,
    TRACE,
  };

  /**
   * Set the logger level.
   *
   * @param lvl Logger::Level VERBOSE logs debug statements, ERR only logs
   *    Status Error's.
   */
  void set_level(Logger::Level lvl);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The logger object. */
  std::shared_ptr<spdlog::logger> logger_;
};

/* ********************************* */
/*              GLOBAL               */
/* ********************************* */

/** Global logger function. */
Logger& global_logger();

}  // namespace common
}  // namespace tiledb

// Also include the public-permissible logger functions here.
#include "tiledb/common/logger_public.h"

#endif  // TILEDB_LOGGER_H
