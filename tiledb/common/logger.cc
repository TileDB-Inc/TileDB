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

Logger::Logger(const std::string& name)
    : name_(name) {
  logger_ = spdlog::get(name);
  if (logger_ == nullptr) {
#ifdef _WIN32
    logger_ = spdlog::stdout_logger_mt(name);
#else
    logger_ = spdlog::stdout_color_mt(name);
#endif
  }
  // Set the default logging format
  // [Year-month-day 24hr-min-second.microsecond]
  // [Process: id]
  // [log level]
  // [logger name]
  // text to log...
  logger_->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [Process: %P] [%l] [%n] %v");
  logger_->set_level(spdlog::level::err);
}

Logger::Logger(std::shared_ptr<spdlog::logger> logger) {
  logger_ = std::move(logger);
}

Logger::~Logger() {
  spdlog::drop("tiledb");
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

void Logger::set_name(const std::string& name) {
  name_ = name;
}

tdb_shared_ptr<Logger> Logger::clone(const std::string& name) {
  std::string new_name = name_ + "] [" + name;
  auto new_loger = tdb_make_shared(Logger, Logger(logger_->clone(new_name)));
  new_loger->set_name(new_name);
  return new_loger;
}

/* ********************************* */
/*              GLOBAL               */
/* ********************************* */

Logger& global_logger() {
  static Logger l("");
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
