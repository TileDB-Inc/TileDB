/**
 * @file   logger.cc
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
 * This file implements class Logger.
 */

#include "logger.h"

namespace tiledb {

Logger::Logger() {
  logger_ = spdlog::get("tiledb");
  if (logger_ == nullptr) {
    logger_ = spdlog::stdout_color_mt("tiledb");
  }
  // Set the default logging format
  // [Year-month-day 24hr-min-second.microsecond]
  // [logger]
  // [Process: id]
  // [Thread: id]
  // [log level]
  // text to log...
  logger_->set_pattern(
      "[%Y-%m-%d %H:%M:%S.%e] [%n] [Process: %P] [Thread: %t] [%l] %v");

#ifdef TILEDB_VERBOSE
  logger_->set_level(spdlog::level::err);
#else
  logger_->set_level(spdlog::level::critical);
#endif
};

Logger::~Logger() {
  spdlog::drop("tiledb");
}

Logger& global_logger() {
  static Logger l;
  return l;
}

// definition for logging status objects
std::ostream& operator<<(std::ostream& os, const Status& st) {
  return os << st.to_string();
}

}  // namespace tiledb
