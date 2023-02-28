/**
 * @file   logger.cc
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
 * This file defines class Logger, declared in logger.h, and the public logging
 * functions, declared in logger_public.h.
 */

#include "tiledb/common/logger.h"
#include "tiledb/platform/platform.h"

#include <spdlog/fmt/fmt.h>
#include <spdlog/fmt/ostr.h>
#include <spdlog/sinks/stdout_sinks.h>
#ifndef _WIN32
#include <spdlog/sinks/stdout_color_sinks.h>
#endif

namespace tiledb::common {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

static inline bool stricmp(const std::string& lhs, const std::string& rhs);
static inline bool charicmp(char lhs, char rhs);

Logger::Logger(Logger::Format format)
  : name_(global_logger_name(format))
  , root_(true) {
  logger_ = spdlog::get(name_);
  if (logger_ == nullptr) {
    if constexpr (platform::is_os_windows) {
      logger_ = spdlog::stdout_logger_mt(name_);
    } else {
      logger_ = spdlog::stdout_color_mt(name_);
    }
  }

  set_format(format);
  set_level(Logger::Level::ERR);
}

Logger::Logger(const sm::Config& config, const std::string& name)
    : name_(name)
    , root_(false) {
  logger_ = spdlog::get(name_);
  if (logger_ == nullptr) {
    if constexpr (platform::is_os_windows) {
      logger_ = spdlog::stdout_logger_mt(name_);
    } else {
      logger_ = spdlog::stdout_color_mt(name_);
    }
  }

  auto format = config.get<std::string>("config.logging_format");
  set_format(format.value_or("DEFAULT"));

  auto level = config.get<std::string>("config.logging_level");
  set_level(level.value_or("ERR"));

  if (root_ && fmt_ == Logger::Format::JSON) {
    // If this is the first logger created set up the opening brace and an
    // array named "log"
    logger_->set_pattern("{\n \"log\": [");
    logger_->critical("");
  }

  // HACK: We need to eventually move this setup to a library wide
  // initialization section. Calling it here each time we construct
  // a logger matches the previous behavior.
  global_logger(fmt_).set_level(level.value_or("ERR"));
}

Logger::Logger(shared_ptr<spdlog::logger> logger) {
  logger_ = std::move(logger);
}

Logger::~Logger() {
  if (root_ && fmt_ == Logger::Format::JSON) {
    // If this is the root/global logger being destroyed, then we output
    // "Logging finished"
    std::string last_log_pattern = {
        "{\"severity\":\"%l\",\"timestamp\":\"%Y-%m-%dT%H:%M:%S.%f%z\","
        "\"process\":\"%P\",\"name\":{%n},\"message\":\"%v\"}"};
    logger_->set_pattern(last_log_pattern);
    // Log a last entry
    logger_->critical("Finished logging.");
    // set the last pattern to close out the "log" json array and the closing
    // brace
    logger_->set_pattern("]\n}");
    logger_->critical("");
  }
  spdlog::drop(name_);
}

void Logger::trace(const char* msg) {
  logger_->trace(msg);
}

void Logger::debug(const char* msg) {
  logger_->debug(msg);
}

void Logger::info(const char* msg) {
  logger_->info(msg);
}

void Logger::warn(const char* msg) {
  logger_->warn(msg);
}

void Logger::error(const char* msg) {
  logger_->error(msg);
}

void Logger::critical(const char* msg) {
  logger_->critical(msg);
}

void Logger::fatal(const char* msg) {
  logger_->error(msg);
  exit(1);
}

Status Logger::status(const Status& st) {
  logger_->error(st.message());
  return st;
}

void Logger::status_no_return_value(const Status& st) {
  logger_->error(st.message());
}

void Logger::trace(const std::string& msg) {
  logger_->trace(msg);
}

void Logger::debug(const std::string& msg) {
  logger_->debug(msg);
}

void Logger::info(const std::string& msg) {
  logger_->info(msg);
}

void Logger::warn(const std::string& msg) {
  logger_->warn(msg);
}

void Logger::error(const std::string& msg) {
  logger_->error(msg);
}

void Logger::critical(const std::string& msg) {
  logger_->critical(msg);
}

void Logger::fatal(const std::string& msg) {
  logger_->error(msg);
  exit(1);
}

void Logger::trace(const std::stringstream& msg) {
  logger_->trace(msg.str());
}

void Logger::debug(const std::stringstream& msg) {
  logger_->debug(msg.str());
}

void Logger::info(const std::stringstream& msg) {
  logger_->info(msg.str());
}

void Logger::warn(const std::stringstream& msg) {
  logger_->warn(msg.str());
}

void Logger::error(const std::stringstream& msg) {
  logger_->error(msg.str());
}

void Logger::critical(const std::stringstream& msg) {
  logger_->critical(msg.str());
}

void Logger::fatal(const std::stringstream& msg) {
  logger_->error(msg.str());
  exit(1);
}

void Logger::set_level(Logger::Level lvl) {
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

void Logger::set_level(const std::string& lvl) noexcept {
  try {
    auto opt_lvl = level_from_str(lvl);
    if (opt_lvl.has_value()) {
      set_level(opt_lvl.value());
    } else {
      error("Ignoring invalid log level: " + lvl);
    }
  } catch (...) {
    error("Unknown error while  setting log level from: " + lvl);
  }
}

void Logger::set_format(Logger::Format fmt) {
  switch (fmt) {
    case Logger::Format::JSON: {
      /*
       * Set up the JSON format
       * {
       *  "severity": "log level",
       *  "timestamp": ISO 8601 time/date format,
       *  "process": "id",
       *  "name": {
       *    "Context": "uid",
       *    "Query": "uid",
       *    "Writer": "uid"
       *  },
       *  "message": "text to log"
       * },
       */
      std::string json_pattern = {
          "{\"severity\":\"%l\",\"timestamp\":\"%Y-%m-%dT%H:%M:%S.%f%z\","
          "\"process\":\"%P\",\"name\":{%n},\"message\":\"%v\"},"};
      logger_->set_pattern(json_pattern);
      break;
    }
    default: {
      /*
       * Set up the default logging format
       * [Year-month-day 24hr-min-second.microsecond]
       * [Process: id]
       * [log level]
       * [logger name]
       * text to log
       */
      std::string default_pattern =
          "[%Y-%m-%d %H:%M:%S.%e] [Process: %P] [%l] [%n] %v";
      logger_->set_pattern(default_pattern);
      break;
    }
  }
  fmt_ = fmt;
}

void Logger::set_format(const std::string& fmt) noexcept {
  try {
    auto opt_fmt = format_from_str(fmt);
    if (opt_fmt.has_value()) {
      set_format(opt_fmt.value());
    } else {
      error("Ignoring invalid log format: " + fmt);
    }
  } catch (...) {
    error("Unknown error while setting log format from: " + fmt);
  }
}

std::optional<Logger::Level> Logger::level_from_str(const std::string& lvl) noexcept {
  try {
    if (stricmp(lvl, "0") || stricmp(lvl, "FATAL")) {
      return {Logger::Level::FATAL};
    } else if (stricmp(lvl, "1") || stricmp(lvl, "ERR")) {
      return {Logger::Level::ERR};
    } else if (stricmp(lvl, "2") || stricmp(lvl, "WARN")) {
      return {Logger::Level::WARN};
    } else if (stricmp(lvl, "3") || stricmp(lvl, "INFO")) {
      return {Logger::Level::INFO};
    } else if (stricmp(lvl, "4") || stricmp(lvl, "DBG")) {
      return {Logger::Level::DBG};
    } else if (stricmp(lvl, "5") || stricmp(lvl, "TRACE")) {
      return {Logger::Level::TRACE};
    } else {
      return std::nullopt;
    }
  } catch (...) {
    return std::nullopt;
  }
}

std::optional<Logger::Format> Logger::format_from_str(const std::string& format) noexcept {
  try {
    if (stricmp(format, "DEFAULT")) {
      return {Logger::Format::DEFAULT};
    } else if (stricmp(format, "JSON")) {
      return {Logger::Format::JSON};
    } else {
      return std::nullopt;
    }
  } catch (...) {
    return std::nullopt;
  }
}

void Logger::set_name(const std::string& tags) {
  name_ = tags;
}

shared_ptr<Logger> Logger::clone(const std::string& tag, uint64_t id) {
  std::string new_tags = add_tag(tag, id);
  auto new_logger = make_shared<Logger>(HERE(), logger_->clone(new_tags));
  new_logger->set_name(new_tags);
  return new_logger;
}

std::string Logger::add_tag(const std::string& tag, uint64_t id) {
  std::string tags;
  switch (fmt_) {
    case Logger::Format::JSON: {
      tags = name_.empty() ? fmt::format("\"{}\":\"{}\"", tag, id) :
                             fmt::format("{},\"{}\":\"{}\"", name_, tag, id);
      break;
    }
    default:
      tags = name_.empty() ? fmt::format("{}: {}", tag, id) :
                             fmt::format("{}] [{}: {}", name_, tag, id);
  }
  return tags;
}

std::string Logger::global_logger_name(Logger::Format format) {
  auto ts_micro =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  return (format == Logger::Format::JSON) ?
          "\"" + std::to_string(ts_micro) + "-Global\":\"1\"" :
          std::to_string(ts_micro) + "-Global";
}

/* ********************************* */
/*              GLOBAL               */
/* ********************************* */

Logger& global_logger(Logger::Format format) {
  static Logger l(format);
  return l;
}

/** Logs a trace. */
void LOG_TRACE(const std::string& msg) {
  global_logger().trace(msg);
}

/** Logs debug. */
void LOG_DEBUG(const std::string& msg) {
  global_logger().debug(msg);
}

/** Logs info. */
void LOG_INFO(const std::string& msg) {
  global_logger().info(msg);
}

/** Logs an warning. */
void LOG_WARN(const std::string& msg) {
  global_logger().warn(msg);
}

/** Logs an error. */
void LOG_ERROR(const std::string& msg) {
  global_logger().error(msg);
}

/** Logs a status. */
Status LOG_STATUS(const Status& st) {
  global_logger().error(st.to_string());
  return st;
}

/** Logs a status without returning it. */
void LOG_STATUS_NO_RETURN_VALUE(const Status& st) {
  global_logger().error(st.to_string());
}

/** Logs a status. */
void LOG_STATUS(const StatusException& se) {
  global_logger().error(se.what());
}

/** Logs a trace. */
void LOG_TRACE(const std::stringstream& msg) {
  global_logger().trace(msg);
}

/** Logs debug. */
void LOG_DEBUG(const std::stringstream& msg) {
  global_logger().debug(msg);
}

/** Logs info. */
void LOG_INFO(const std::stringstream& msg) {
  global_logger().info(msg);
}

/** Logs a warning. */
void LOG_WARN(const std::stringstream& msg) {
  global_logger().warn(msg);
}

/** Logs an error. */
void LOG_ERROR(const std::stringstream& msg) {
  global_logger().error(msg);
}


static inline bool stricmp(const std::string& lhs, const std::string& rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }

  for(size_t i = 0; i < lhs.size(); i++) {
    if (charicmp(lhs[i], rhs[i])) {
      return false;
    }
  }

  return true;
}

static inline bool charicmp(char lhs, char rhs) {
  if (lhs >= 'A' && lhs <= 'Z') {
    lhs = 'a' + (lhs - 'A');
  }
  if (rhs >= 'A' && rhs <= 'Z') {
    rhs = 'a' + (rhs - 'A');
  }
  return lhs == rhs;
}


}  // namespace tiledb::common
