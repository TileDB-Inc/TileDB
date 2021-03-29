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
 * This file defines class Logger.
 */

#pragma once

#ifndef TILEDB_LOGGER_H
#define TILEDB_LOGGER_H

#include <spdlog/fmt/ostr.h>
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
  void trace(const char* msg) {
    logger_->trace(msg);
  }

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
  void debug(const char* msg) {
    logger_->debug(msg);
  }

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
  void info(const char* msg) {
    logger_->info(msg);
  }

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
  void warn(const char* msg) {
    logger_->warn(msg);
  }

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

  /**
   * Log a critical statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void critical(const char* msg) {
    logger_->critical(msg);
  }

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
  void set_level(Logger::Level lvl) {
    switch (lvl) {
      case Logger::Level::FATAL:
        logger_->set_level(spdlog::level::critical);
        break;
      case Logger::Level::ERR:
        logger_->set_level(spdlog::level::err);
        break;
      case Logger::Level::WARN:
        logger_->set_level(spdlog::level::warn);
        break;
      case Logger::Level::INFO:
        logger_->set_level(spdlog::level::info);
        break;
      case Logger::Level::DBG:
        logger_->set_level(spdlog::level::debug);
        break;
      case Logger::Level::TRACE:
        logger_->set_level(spdlog::level::trace);
        break;
    }
  }

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

/** Logs an error. */
inline void LOG_TRACE(const std::string& msg) {
  global_logger().trace(msg.c_str());
}

/** Logs an error. */
inline void LOG_DEBUG(const std::string& msg) {
  global_logger().debug(msg.c_str());
}

/** Logs an error. */
inline void LOG_INFO(const std::string& msg) {
  global_logger().info(msg.c_str());
}

/** Logs an error. */
inline void LOG_WARN(const std::string& msg) {
  global_logger().warn(msg.c_str());
}

/** Logs an error. */
inline void LOG_ERROR(const std::string& msg) {
  global_logger().error(msg.c_str());
}

/** Logs a status. */
inline Status LOG_STATUS(const Status& st) {
  global_logger().error(st.to_string().c_str());
  return st;
}

/** Logs a status. */
inline Status LOG_STATUS(const Status& st, Logger::Level level) {
  switch (level) {
    case Logger::Level::FATAL:
      global_logger().critical(st.to_string().c_str());
      break;
    case Logger::Level::ERR:
      global_logger().error(st.to_string().c_str());
      break;
    case Logger::Level::WARN:
      global_logger().warn(st.to_string().c_str());
      break;
    case Logger::Level::INFO:
      global_logger().info(st.to_string().c_str());
      break;
    case Logger::Level::DBG:
      global_logger().debug(st.to_string().c_str());
      break;
    case Logger::Level::TRACE:
      global_logger().trace(st.to_string().c_str());
      break;
  }
  return st;
}

/** Logs an error and exits with a non-zero status. */
inline void LOG_FATAL(const std::string& msg) {
  global_logger().error(msg.c_str());
  exit(1);
}

}  // namespace common
}  // namespace tiledb

#endif  // TILEDB_LOGGER_H
