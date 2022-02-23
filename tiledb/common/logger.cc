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

template <class P>
LoggerImpl<P>::LoggerImpl(
    const std::string& name, const LoggerImpl<P>::Format format, const bool root)
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
  if (root && format == LoggerImpl<P>::Format::JSON) {
    // If this is the first logger created set up the opening brace and an
    // array named "log"
    logger_->set_pattern("{\n \"log\": [");
    logger_->critical("");
  }
  set_level(LoggerImpl<P>::Level::ERR);
  set_format(format);
}

template <class P>
LoggerImpl<P>::LoggerImpl(tdb_shared_ptr<spdlog::logger> logger) {
  logger_ = std::move(logger);
}

template <class P>
LoggerImpl<P>::~LoggerImpl() {
  if (root_ && fmt_ == LoggerImpl<P>::Format::JSON) {
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

template <class P>
void LoggerImpl<P>::trace(const char* msg) {
  logger_->trace(msg);
}

template <class P>
void LoggerImpl<P>::debug(const char* msg) {
  logger_->debug(msg);
}

template <class P>
void LoggerImpl<P>::info(const char* msg) {
  logger_->info(msg);
}

template <class P>
void LoggerImpl<P>::warn(const char* msg) {
  logger_->warn(msg);
}

template <class P>
void LoggerImpl<P>::error(const char* msg) {
  logger_->error(msg);
}

template <class P>
void LoggerImpl<P>::critical(const char* msg) {
  logger_->critical(msg);
}

template <class P>
void LoggerImpl<P>::fatal(const char* msg) {
  logger_->error(msg);
  exit(1);
}

template <class P>
Status LoggerImpl<P>::status(const Status& st) {
  logger_->error(st.message());
  return st;
}

template <class P>
void LoggerImpl<P>::trace(const std::string& msg) {
  logger_->trace(msg);
}

template <class P>
void LoggerImpl<P>::debug(const std::string& msg) {
  logger_->debug(msg);
}

template <class P>
void LoggerImpl<P>::info(const std::string& msg) {
  logger_->info(msg);
}

template <class P>
void LoggerImpl<P>::warn(const std::string& msg) {
  logger_->warn(msg);
}

template <class P>
void LoggerImpl<P>::error(const std::string& msg) {
  logger_->error(msg);
}

template <class P>
void LoggerImpl<P>::critical(const std::string& msg) {
  logger_->critical(msg);
}

template <class P>
void LoggerImpl<P>::fatal(const std::string& msg) {
  logger_->error(msg);
  exit(1);
}

template <class P>
void LoggerImpl<P>::trace(const std::stringstream& msg) {
  logger_->trace(msg.str());
}

template <class P>
void LoggerImpl<P>::debug(const std::stringstream& msg) {
  logger_->debug(msg.str());
}

template <class P>
void LoggerImpl<P>::info(const std::stringstream& msg) {
  logger_->info(msg.str());
}

template <class P>
void LoggerImpl<P>::warn(const std::stringstream& msg) {
  logger_->warn(msg.str());
}

template <class P>
void LoggerImpl<P>::error(const std::stringstream& msg) {
  logger_->error(msg.str());
}

template <class P>
void LoggerImpl<P>::critical(const std::stringstream& msg) {
  logger_->critical(msg.str());
}

template <class P>
void LoggerImpl<P>::fatal(const std::stringstream& msg) {
  logger_->error(msg.str());
  exit(1);
}

template <class P>
void LoggerImpl<P>::set_level(LoggerImpl<P>::Level lvl) {
  switch (lvl) {
    case LoggerImpl<P>::Level::FATAL:
      logger_->set_level(spdlog::level::critical);
      break;
    case LoggerImpl<P>::Level::ERR:
      logger_->set_level(spdlog::level::err);
      break;
    case LoggerImpl<P>::Level::WARN:
      logger_->set_level(spdlog::level::warn);
      break;
    case LoggerImpl<P>::Level::INFO:
      logger_->set_level(spdlog::level::info);
      break;
    case LoggerImpl<P>::Level::DBG:
      logger_->set_level(spdlog::level::debug);
      break;
    case LoggerImpl<P>::Level::TRACE:
      logger_->set_level(spdlog::level::trace);
      break;
  }
}

template <class P>
void LoggerImpl<P>::set_format(LoggerImpl<P>::Format fmt) {
  switch (fmt) {
    case LoggerImpl<P>::Format::JSON: {
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

template <class P>
void LoggerImpl<P>::set_name(const std::string& tags) {
  name_ = tags;
}

template <class P>
tdb_shared_ptr<LoggerImpl<P>> LoggerImpl<P>::clone(const std::string& tag, uint64_t id) {
  std::string new_tags = add_tag(tag, id);
  auto new_logger =
      tiledb::common::make_shared<LoggerImpl<P>>(HERE(), logger_->clone(new_tags));
  new_logger->set_name(new_tags);
  return new_logger;
}

template <class P>
std::string LoggerImpl<P>::add_tag(const std::string& tag, uint64_t id) {
  std::string tags;
  switch (fmt_) {
    case LoggerImpl<P>::Format::JSON: {
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

template<class P>
LoggerImpl<P>& global_logger(
    typename LoggerImpl<P>::Format format) {
    static std::string name = (format == LoggerImpl<P>::Format::JSON) ?
                                LoggerImpl<P>::global_logger_json_name :
                                LoggerImpl<P>::global_logger_default_name;
  static LoggerImpl<P> l(name, format, true);
  return l;
}

template class LoggerImpl<NullLoggerPolicy>;
template class LoggerImpl<test::LoggerTestPolicy>;

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

/** Logs an error and exits with a non-zero status. */
void LOG_FATAL(const std::string& msg) {
  global_logger().error(msg);
  exit(1);
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

/** Logs an error and exits with a non-zero status. */
void LOG_FATAL(const std::stringstream& msg) {
  global_logger().error(msg);
  exit(1);
}

}  // namespace tiledb::common
