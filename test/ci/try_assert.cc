#include <cassert>
#include <filesystem>
#include <iostream>

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#include "tiledb/common/assert.h"
#include "tiledb/common/logger.h"

#if defined(_MSC_VER)
#include <crtdbg.h>
#endif

int main(int argc, char** argv) {
  std::optional<tiledb::common::Logger> log;
  if (argc > 1) {
    const auto path = std::filesystem::path(std::string(argv[0]));
    const std::string log_name = path.filename();
    const auto logfile = std::filesystem::path(std::string(argv[1]));

    auto spdlogger = spdlog::basic_logger_mt(log_name, logfile);
    log.emplace(spdlogger);
  }

#if defined(_MSC_VER)
  // We disable the following events on abort:
  // _WRITE_ABORT_MSG: Display message box with Abort, Retry, Ignore
  // _CALL_REPORTFAULT: Send an error report to Microsoft
  // The second parameter specifies which flags to change, and the first
  // the value of these flags.
  _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
  // Configures assert() failures to write message to stderr and fail-fast.
  _CrtSetReportMode(_CRT_ASSERT, _CRTDBG_MODE_FILE);
  _CrtSetReportFile(_CRT_ASSERT, _CRTDBG_FILE_STDERR);
#endif
  if (log.has_value()) {
    log.value().error("begin passert(false)");
  }

  passert(false);

  if (log.has_value()) {
    log.value().error("end passert(false)");
  }

  std::cout << "Assert did not exit!" << std::endl;
  return 0;
}
