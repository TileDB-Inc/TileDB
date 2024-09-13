//              Copyright Catch2 Authors
// Distributed under the Boost Software License, Version 1.0.
//   (See accompanying file LICENSE.txt or copy at
//        https://www.boost.org/LICENSE_1_0.txt)

// SPDX-License-Identifier: BSL-1.0
#include <catch2/catch_session.hpp>
#include <catch2/internal/catch_compiler_capabilities.hpp>
#include <catch2/internal/catch_config_wchar.hpp>
#include <catch2/internal/catch_leak_detector.hpp>
#include <catch2/internal/catch_platform.hpp>

#include <cstdlib>

namespace Catch {
CATCH_INTERNAL_START_WARNINGS_SUPPRESSION
CATCH_INTERNAL_SUPPRESS_GLOBALS_WARNINGS
static LeakDetector leakDetector;
CATCH_INTERNAL_STOP_WARNINGS_SUPPRESSION
}  // namespace Catch

// Allow users of amalgamated .cpp file to remove our main and provide their
// own.
#if !defined(CATCH_AMALGAMATED_CUSTOM_MAIN)

#if defined(CATCH_CONFIG_WCHAR) && defined(CATCH_PLATFORM_WINDOWS) && \
    defined(_UNICODE) && !defined(DO_NOT_USE_WMAIN)
// Standard C/C++ Win32 Unicode wmain entry point
extern "C" int __cdecl wmain(int argc, wchar_t* argv[], wchar_t*[]) {
#else
// Standard C/C++ main entry point
int main(int argc, char* argv[]) {
#endif
#if defined(_MSC_VER)
  // We disable the following events on abort in CI environments:
  // _WRITE_ABORT_MSG: Display message box with Abort, Retry, Ignore
  // _CALL_REPORTFAULT: Send an error report to Microsoft
  // The second parameter specifies which flags to change, and the first
  // the value of these flags.
  if (std::getenv("CI") != nullptr) {
    _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
  }
#endif

  // We want to force the linker not to discard the global variable
  // and its constructor, as it (optionally) registers leak detector
  (void)&Catch::leakDetector;

  return Catch::Session().run(argc, argv);
}

#endif  // !defined(CATCH_AMALGAMATED_CUSTOM_MAIN
