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
      "[%Y-%m-%d %H:%M:S.%e] [%n] [Process: %P] [Thread: %t] [%l] %v");

#ifdef TILEDB_VERBOSE
  logger_->set_level(spdlog::level::err);
#else
  logger_->set_level(spdlog::level::critical);
#endif
};

Logger::~Logger() {
  spdlog::drop("tiledb");
}

// definition for logging status objects
std::ostream& operator<<(std::ostream& os, const Status& st) {
  return os << st.to_string();
}

}  // namespace tiledb
