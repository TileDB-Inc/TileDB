/**
 * @file   logger_distinct.h
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
 * This file defines class LoggerDistinct to create uniquely identified loggers
 * for classes
 */

#ifndef TILEDB_LOGGER_DISTINCT_H
#define TILEDB_LOGGER_DISTINCT_H

#include <atomic>

#include "tiledb/common/common.h"

#include "logger.h"

namespace tiledb {

namespace common {

/**
 * LoggerDistinct class uses CRTP pattern in order for each derived class to
 * have its own static variable for counting instances.
 */
template <typename T>
class LoggerDistinct : public Logger {
 public:
  /* ****************************** */
  /*   CONSTRUCTORS & DESTRUCTORS   */
  /* ****************************** */

  /**
   * Constructor.
   * Constructs a new logger root
   */
  LoggerDistinct(const std::string& name)
      : Logger(name + ": " + std::to_string(increment_instance_id())){};

  /**
   * Constructor.
   * Constructs a new child logger from a parent
   */
  LoggerDistinct(
      const std::string& new_tag, const shared_ptr<Logger>& parent_log)
      : Logger(parent_log->logger_->clone(new_child_tags(
            parent_log->name_, new_tag, increment_instance_id()))) {
    set_name(tags_);
    instance_count_++;
  }

  /** Destructor. */
  ~LoggerDistinct() = default;

  /** Copy constructor. */
  DISABLE_COPY(LoggerDistinct);

  /** Move constructor. */
  DISABLE_MOVE(LoggerDistinct);

  /* ****************************** */
  /*           OPERATORS            */
  /* ****************************** */

  /** Copy-assignment. */
  DISABLE_COPY_ASSIGN(LoggerDistinct);

  /** Move-assignment. */
  DISABLE_MOVE_ASSIGN(LoggerDistinct);

  /* ****************************** */
  /*              API               */
  /* ****************************** */

  static uint64_t increment_instance_id() {
    return ++id_;
  }

 private:
  /** UID of the logger instance */
  static inline std::atomic<uint64_t> id_ = 0;

  static inline std::string tags_;

  /** Add a tag to the logger name */
  std::string new_child_tags(
      const std::string& old_tags,
      const std::string& new_tag,
      uint64_t tag_id) {
    switch (fmt_) {
      case Logger::Format::JSON: {
        tags_ = old_tags.empty() ?
                    fmt::format("\"{}\":\"{}\"", new_tag, tag_id) :
                    fmt::format("{},\"{}\":\"{}\"", old_tags, new_tag, tag_id);
        break;
      }
      default:
        tags_ = old_tags.empty() ?
                    fmt::format("{}: {}", new_tag, tag_id) :
                    fmt::format("{}] [{}: {}", old_tags, new_tag, tag_id);
    }
    return tags_;
  }
};

}  // namespace common
}  // namespace tiledb

#endif  // TILEDB_LOGGER_DISTINCT_H