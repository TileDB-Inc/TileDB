/**
 * @file   log_duration.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file defines class `LogDuration`.
 */

#ifndef TILEDB_LOG_DURATION_H
#define TILEDB_LOG_DURATION_H

#include <spdlog/fmt/fmt.h>

#include "tiledb/common/logger.h"

namespace tiledb::common {

/**
 * Emits log messages on the start and end of a scope, noting the scope's
 * duration.
 */
class LogDuration {
 public:
  LogDuration() = delete;

  DISABLE_COPY_AND_COPY_ASSIGN(LogDuration);
  DISABLE_MOVE_AND_MOVE_ASSIGN(LogDuration);

  /**
   * Constructor.
   *
   * The log level is set to TRACE.
   *
   * @param logger The logger to use.
   * @param fmt The format string to describe the operation.
   * @param args Arguments for the format string.
   */
  template <typename... Args>
  constexpr LogDuration(
      Logger* logger,
      // fmt::format_string<Args...> fmt,
      const std::string& fmt,
      Args&&... args)
      : LogDuration(
            logger,
            Logger::Level::TRACE,
            std::move(fmt),
            std::forward<Args>(args)...) {
  }

  /**
   * Constructor.
   *
   * @param logger The logger to use.
   * @param level The level to log the event.
   * @param fmt The format string to describe the operation.
   * @param args Arguments for the format string.
   */
  template <typename... Args>
  constexpr LogDuration(
      Logger* logger,
      Logger::Level level,
      // fmt::format_string<Args...> fmt,
      const std::string& fmt,
      Args&&... args)
      : level_(level) {
    assert(logger);

    if (logger->should_log(level_)) {
      logger_ = logger;
      // event_name_ = fmt::format(fmt, std::forward<Args>(args)...);
      // Forwarding args in make_format_args causes an error.
      event_name_ = fmt::vformat(fmt, fmt::make_format_args(args...));
      logger_->log(level_, "{} started", event_name_);
      start_ = std::chrono::high_resolution_clock::now();
    }
  }

  ~LogDuration() {
    if (logger_) {
      auto end = std::chrono::high_resolution_clock::now();
      std::chrono::duration<float, std::chrono::milliseconds::period> duration =
          end - start_;
      logger_->log(level_, "{} took {} ms", event_name_, duration.count());
    }
  }

 private:
  Logger::Level level_;
  /**
   * A pointer to the logger to use for logging.
   *
   * A null pointer indicates that the logger is not enabled for the given
   * level and no event should be emitted.
   */
  Logger* logger_;
  std::chrono::time_point<std::chrono::high_resolution_clock> start_;
  std::string event_name_;
};

}  // namespace tiledb::common

#endif  // TILEDB_LOG_DURATION_H
