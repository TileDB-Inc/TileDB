#pragma once

#include "spdlog/fmt/ostr.h"
#include "spdlog/spdlog.h"
#include "status.h"

#ifndef TILEDB_LOGGER_H
#define TILEDB_LOGGER_H

namespace tiledb {

class Logger {
  enum class Level : char {
    VERBOSE,
    ERROR,
  };

 public:
  /** Constructor. */
  Logger();

  /** Destructor. */
  ~Logger();

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
#endif  // TILEDB_LOGGER_H
