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

#ifndef __TILEDB_LOGGER_H__
#define __TILEDB_LOGGER_H__

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

  // TODO: Doc!

  void debug(const char* msg) {
    logger_->debug(msg);
  }

  template <typename Arg1, typename... Args>
  void debug(const char* fmt, const Arg1& arg1, const Args&... args) {
    logger_->debug(fmt, arg1, args...);
  }

  void error(const char* msg) {
    logger_->error(msg);
  }

  template <typename Arg1, typename... Args>
  void error(const char* fmt, const Arg1& arg1, const Args&... args) {
    logger_->error(fmt, arg1, args...);
  }

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

#ifdef TILEDB_VERBOSE
inline void LOG_ERROR(const std::string& msg) {
  spdlog::get("tiledb")->error(msg);
}

inline Status LOG_STATUS(const Status& st) {
  spdlog::get("tiledb")->error(st.to_string());
  return st;
}
#else
inline void LOG_ERROR(const std::string& msg) {
}

inline Status LOG_STATUS(const Status& st) {
  return st;
}
#endif

}  // namespace tiledb

#endif  // __TILEDB_LOGGER_H__
