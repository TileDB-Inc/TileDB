#include <iostream>
#include <vector>

#if defined(_MSC_VER)
#include <crtdbg.h>
#endif

int main(int, char**) {
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
  std::vector<int> values;

  // this should exit with out of bounds assert if that is enabled
  std::cout << "&values[0] = " << &values[0] << std::endl;

  return 0;
}
