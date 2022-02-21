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
#include <atomic>
#include <sstream>

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/status.h"

namespace tiledb {
namespace common {

/** Definition of class Logger. */
class Logger {
 public:
  enum class Format : char;
  enum class Level : char;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructors */
  Logger(
      const std::string& name,
      const Logger::Format format = Logger::Format::DEFAULT,
      const bool root = false);

  Logger(tdb_shared_ptr<spdlog::logger> logger);

  /** Destructor. */
  ~Logger();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Clone the current logger and get a new object with
   * the same configurations
   *
   * @param name The name of the new logger
   * @param id An id to use as suffix for the name of the new logger
   */
  tdb_shared_ptr<Logger> clone(const std::string& name, uint64_t id);

  /**
   * Log a trace statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void trace(const char* msg);

  /**
   * Log a trace statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void trace(const std::string& msg);

  /**
   * Log a trace statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void trace(const std::stringstream& msg);

  /**
   * A formatted trace statement.
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
   * Log a debug statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void debug(const std::string& msg);

  /**
   * Log a debug statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void debug(const std::stringstream& msg);

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
   * Log an info statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void info(const std::string& msg);

  /**
   * Log an info statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void info(const std::stringstream& msg);

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
   * Log a warn statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void warn(const std::string& msg);

  /**
   * Log a warn statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void warn(const std::stringstream& msg);

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

  /**
   * Log an error with no message formatting.
   *
   * @param msg The string to log
   * */
  void error(const std::string& msg);

  /**
   * Log an error with no message formatting.
   *
   * @param msg The string to log
   * */
  void error(const std::stringstream& msg);

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
   * Log a critical statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void critical(const std::string& msg);

  /**
   * Log a critical statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void critical(const std::stringstream& msg);

  /**
   * Log a message from a Status object and return
   * the same Status object
   *
   * @param st The Status object to log
   */
  Status status(const Status& st);

  /**
   * Log an error and exit with a non-zero status.
   *
   * @param msg The string to log.
   */
  void fatal(const char* msg);

  /**
   * Log an error and exit with a non-zero status.
   *
   * @param msg The string to log.
   */
  void fatal(const std::string& msg);

  /**
   * Log an error and exit with a non-zero status.
   *
   * @param msg The string to log.
   */
  void fatal(const std::stringstream& msg);

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

  /**
   * Set the logger level.
   *
   * @param lvl Logger::Level VERBOSE logs debug statements, ERR only logs
   *    Status Error's.
   */
  void set_level(Logger::Level lvl);

  /**
   * Set the logger output format.
   *
   * @param fmt Logger::Format JSON logs in json format, DEFAULT
   * logs in the default tiledb format
   */
  void set_format(Logger::Format fmt);

  /* ********************************* */
  /*          PUBLIC ATTRIBUTES        */
  /* ********************************* */

  /** Verbosity level. */
  enum class Level : char {
    FATAL,
    ERR,
    WARN,
    INFO,
    DBG,
    TRACE,
  };

  /** Log format. */
  enum class Format : char {
    DEFAULT,
    JSON,
  };

  /** The name of the global logger */
  static inline constexpr char global_logger_default_name[] = "Global";

  /** The name of the global logger in json format */
  static inline constexpr char global_logger_json_name[] = "\"Global\":\"1\"";

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The logger object. */
  std::shared_ptr<spdlog::logger> logger_;

  /** The name of the logger */
  std::string name_;

  /** The format of the logger  */
  static inline Logger::Format fmt_ = Logger::Format::DEFAULT;

  /** A boolean flag that tells us whether the logger is the statically declared
   * global_logger */
  bool root_ = false;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Set the logger name.
   *
   * @param tags The string to use as a name, usually a
   * concatenation of [tag:id] strings
   */
  void set_name(const std::string& tags);

  /**
   * Create a new string by appending a [tag:id] to the current
   * name of the logger. Does not modify the name of the current
   * logger object.
   *
   * @param tag The string to add in the new tag
   * @param id The id to add in the new tag
   *
   */
  std::string add_tag(const std::string& tag, uint64_t id);
};

/* ********************************* */
/*              GLOBAL               */
/* ********************************* */

/**
 * Returns a global logger to be used for general logging
 *
 * @param format The output format of the logger
 */
Logger& global_logger(Logger::Format format = Logger::Format::DEFAULT);

/**
 * Returns the logger format type given a string representation.
 *
 *  @param format_type_str The string representation of the logger format type
 *  @param[out] format_type The logger format type
 */
inline Status logger_format_from_string(
    const std::string& format_type_str, Logger::Format* format_type) {
  if (format_type_str == "DEFAULT")
    *format_type = Logger::Format::DEFAULT;
  else if (format_type_str == "JSON")
    *format_type = Logger::Format::JSON;
  else {
    return Status_Error("Unsupported logging format: " + format_type_str);
  }
  return Status::Ok();
}

}  // namespace common
}  // namespace tiledb

// Also include the public-permissible logger functions here.
#include "tiledb/common/logger_public.h"

#endif  // TILEDB_LOGGER_H
