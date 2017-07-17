/**
 * @file   logger.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file defines class Logger.
 */

#pragma once

#ifndef TILEDB_LOGGER_H
#define TILEDB_LOGGER_H

#include "spdlog/fmt/ostr.h"
#include "spdlog/spdlog.h"
#include "status.h"

namespace tiledb {

/** Definition of class Logger. */
class Logger {
  /** Verbosity level. */
  enum class Level : char {
    VERBOSE,
    ERROR,
  };

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

  /** log an debug statement with no message formatting
   *
   * @param msg The string to log
   */
  void debug(const char* msg) {
    logger_->debug(msg);
  }

  /** A formatted debug statment.
   *
   * @param fmt A fmtlib format string, see http://fmtlib.net/latest/ for
   * details.
   * @param arg positional argument to format.
   * @param args optional additional positional arguments to format.
   */
  template <typename Arg1, typename... Args>
  void debug(const char* fmt, const Arg1& arg1, const Args&... args) {
    logger_->debug(fmt, arg1, args...);
  }

  /** log an error with no message formatting
   *
   * @param msg The string to log
   * */
  void error(const char* msg) {
    logger_->error(msg);
  }

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

  /** Set the logger level.
   *
   * @param lvl  Logger::Level VERBOSE logs debug statements, ERROR only logs
   * Status Error's.
   */
  void set_level(Logger::Level lvl) {
    switch (lvl) {
      case Logger::Level::VERBOSE:
        logger_->set_level(spdlog::level::debug);
        break;
      case Logger::Level::ERROR:
        logger_->set_level(spdlog::level::err);
        break;
    }
  }

  /** Returns whether the logger should log a message given the currently set
   * log level.
   *
   * @param lvl The Logger::Level to test
   * @return bool true, if the logger will log the given Logger::Level, false
   * otherwise.
   */
  bool should_log(Logger::Level lvl) {
    switch (lvl) {
      case Logger::Level::VERBOSE:
        return logger_->should_log(spdlog::level::debug);
      case Logger::Level::ERROR:
        return logger_->should_log(spdlog::level::err);
    }
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  std::shared_ptr<spdlog::logger> logger_;
};

Logger& global_logger();

// TODO: these should go away eventually
#ifdef TILEDB_VERBOSE
inline void LOG_ERROR(const std::string& msg) {
  global_logger().error(msg.c_str());
}

inline Status LOG_STATUS(const Status& st) {
  global_logger().error(st.to_string().c_str());
  return st;
}
#else
// no-ops
inline void LOG_ERROR(const std::string& msg){};
inline Status LOG_STATUS(const Status& st) {
  return st;
}
#endif

}  // namespace tiledb

#endif  // TILEDB_LOGGER_H
