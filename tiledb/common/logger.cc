/**
 * @file   logger.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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

#include <spdlog/fmt/fmt.h>
#include <spdlog/fmt/ostr.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

#ifndef _WIN32
#include <spdlog/sinks/stdout_color_sinks.h>
#endif

#include "tiledb/common/logger.h"

namespace tiledb::common {

spdlog::level::level_enum to_spdlog_level(Logger::Level lvl) {
  switch (lvl) {
    case Logger::Level::FATAL:
      return spdlog::level::critical;
    case Logger::Level::ERR:
      return spdlog::level::err;
    case Logger::Level::WARN:
      return spdlog::level::warn;
    case Logger::Level::INFO:
      return spdlog::level::info;
    case Logger::Level::DBG:
      return spdlog::level::debug;
    default:
      assert(lvl == Logger::Level::TRACE);
      return spdlog::level::trace;
  }
}

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Logger::Logger(
    const std::string& name,
    const Logger::Level level,
    const Logger::Format format,
    const bool root)
    : name_(name)
    , root_(root) {
  logger_ = spdlog::get(name_);
  if (logger_ == nullptr) {
#ifdef _WIN32
    logger_ = spdlog::stdout_logger_mt(name_);
#else
    logger_ = spdlog::stdout_color_mt(name_);
#endif
  }
  if (root && format == Logger::Format::JSON) {
    // If this is the first logger created set up the opening brace and an
    // array named "log"
    logger_->set_pattern("{\n \"log\": [");
    logger_->critical("");
  }
  set_level(level);
  set_format(format);
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

void Logger::log(Logger::Level level, const char* msg) {
  logger_->log(to_spdlog_level(level), msg);
}

Status Logger::status(const Status& st) {
  logger_->error(st.message());
  return st;
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

void Logger::log(Logger::Level level, const std::string& msg) {
  logger_->log(to_spdlog_level(level), msg);
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

void Logger::log(Logger::Level level, const std::stringstream& msg) {
  logger_->log(to_spdlog_level(level), msg.str());
}

bool Logger::should_log(Logger::Level lvl) {
  return logger_->should_log(to_spdlog_level(lvl));
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

/* ********************************* */
/*              GLOBAL               */
/* ********************************* */

std::string global_logger_name(const Logger::Format format) {
  std::string name{
      std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count()) +
      "-Global"};
  if (format != Logger::Format::JSON) {
    return name;
  }
  return {"\"" + name + "\":\"1\""};
}

Logger& global_logger(Logger::Format format) {
  // Note: We are *not* deallocating this root `Logger` instance so that
  // during process exit, threads other than main will not trigger segfault's
  // if they attempt to log after the main thread has exited.
  static Logger* l = tiledb_new<Logger>(
      HERE(), global_logger_name(format), Logger::Level::ERR, format, true);
  return *l;
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

}  // namespace tiledb::common
