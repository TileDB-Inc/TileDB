/**
 * @file   log_duration_instrument.h
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
 * This file defines class `LogDurationInstrument`.
 */

#ifndef TILEDB_LOG_DURATION_INSTRUMENT_H
#define TILEDB_LOG_DURATION_INSTRUMENT_H

#include <spdlog/fmt/fmt.h>

#include <spdlog/fmt/chrono.h>

#include "tiledb/common/logger.h"

namespace tiledb::common {

/**
 * Emits log messages on the start and end of a scope, noting the scope's
 * duration.
 */
class LogDurationInstrument {
 public:
  LogDurationInstrument() = delete;

  DISABLE_COPY_AND_COPY_ASSIGN(LogDurationInstrument);
  DISABLE_MOVE_AND_MOVE_ASSIGN(LogDurationInstrument);

  /**
   * Constructor.
   *
   * @param logger The logger to use.
   * @param fmt The format string to describe the operation.
   * @param args Arguments for the format string.
   */
  template <typename... Args>
  LogDurationInstrument(
      Logger* logger,
      // Ideally this would be a fmt::format_string<Args...> but it fails with
      // weird constexpr-related compile errors.
      const std::string& fmt,
      Args&&... args) {
    assert(logger);

    if (logger->should_log(DefaultLevel)) {
      logger_ = logger;
      // Forwarding args in make_format_args causes an error.
      event_name_ = fmt::vformat(fmt, fmt::make_format_args(args...));
      start_ = std::chrono::high_resolution_clock::now();
      system_time_start_ = std::chrono::system_clock::now();
    }
  }

  ~LogDurationInstrument() {
    try {
      if (logger_) {
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<float, std::chrono::milliseconds::period>
            duration = end - start_;
        logger_->log(
            DefaultLevel,
            "{} started at {:%Y-%m-%d %X} and lasted {}",
            event_name_,
            system_time_start_,
            duration);
      }
    } catch (...) {
      // Ignore exceptions inside destructor.
    }
  }

 private:
  /** Level used for logging. */
  static constexpr Logger::Level DefaultLevel = Logger::Level::TRACE;

  /**
   * A pointer to the logger to use for logging.
   *
   * A null pointer indicates that the logger is not enabled for the given
   * level and no event should be emitted.
   */
  Logger* logger_;

  /** High-resolution time point of the operation's start. */
  std::chrono::time_point<std::chrono::high_resolution_clock> start_;

  /**
   * System time point of the operation's start.
   *
   * We use it to format the operation's start time.
   */
  std::chrono::time_point<std::chrono::system_clock> system_time_start_;

  /** Name of the event that will be logged. */
  std::string event_name_;
};

}  // namespace tiledb::common

#endif  // TILEDB_LOG_DURATION_INSTRUMENT_H
